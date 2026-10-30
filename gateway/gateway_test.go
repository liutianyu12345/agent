package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gateway/client"
	"gateway/middleware"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// TestRateLimiterFailOpen 测试 1：当 Redis 未启动或连接失败时，限流中间件是否能自动触发 Fail-open 降级放行
func TestRateLimiterFailOpen(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 故意配置一个不可达的 Redis 端口，模拟 Redis 故障
	rdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:63999",
		DialTimeout: 50 * time.Millisecond,
	})

	r := gin.New()
	r.Use(middleware.RedisTokenBucketLimiter(rdb, 2, time.Minute))
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "server": "Go Gateway"})
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("期望状态码 200 (Fail-open 降级放行)，实际得到: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), `"status":"ok"`) {
		t.Fatalf("响应内容不符合预期: %s", w.Body.String())
	}
	t.Logf("✅ Fail-open 降级放行与 /health 探针检验通过: %s", w.Body.String())
}

// TestForwardStreamSSE 测试 2：使用 Mock Server 模拟 Python Agent 的 SSE 流输出，检验网关流式读取
func TestForwardStreamSSE(t *testing.T) {
	// 启动一个内置的假 Python Agent 服务端
	mockPythonServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			t.Errorf("期望请求路径 /stream，实际得到: %s", r.URL.Path)
		}
		var reqBody client.StreamTaskRequest
		_ = json.NewDecoder(r.Body).Decode(&reqBody)
		if reqBody.Prompt == "" {
			t.Errorf("收到空的 Prompt")
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)

		// 模拟分 3 次吐出 SSE 数据块
		chunks := []string{
			`data: {"node": "rag", "status": "PROCESSING"}`,
			`data: {"node": "coder", "status": "PROCESSING"}`,
			`data: [DONE]`,
		}
		for _, ch := range chunks {
			fmt.Fprintf(w, "%s\n\n", ch)
			flusher.Flush()
		}
	}))
	defer mockPythonServer.Close()

	agentCli := client.NewAgentClient(mockPythonServer.URL)
	var received []string

	err := agentCli.ForwardStream(context.Background(), &client.StreamTaskRequest{
		Prompt:   "编写快速排序",
		ThreadID: "test_thread_001",
	}, func(line string) error {
		received = append(received, line)
		return nil
	})

	if err != nil {
		t.Fatalf("ForwardStream 执行出错: %v", err)
	}
	if len(received) != 3 {
		t.Fatalf("期望收到 3 行有效 SSE 事件，实际收到 %d 行: %v", len(received), received)
	}
	t.Logf("✅ SSE 实时流转发检验通过，共接收 %d 条事件: %v", len(received), received)
}

// TestForwardStreamContextCancel 测试 3：模拟客户端中途关闭网页，检验 Context 取消是否能立即终止读取
func TestForwardStreamContextCancel(t *testing.T) {
	mockPythonServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for i := 1; i <= 10; i++ {
			fmt.Fprintf(w, "data: {\"step\": %d}\n\n", i)
			flusher.Flush()
			time.Sleep(30 * time.Millisecond)
		}
	}))
	defer mockPythonServer.Close()

	agentCli := client.NewAgentClient(mockPythonServer.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	err := agentCli.ForwardStream(ctx, &client.StreamTaskRequest{
		Prompt:   "长耗时任务",
		ThreadID: "test_cancel_002",
	}, func(line string) error {
		count++
		if count == 2 {
			// 模拟收到第 2 条消息后客户端主动断开连接
			cancel()
		}
		return nil
	})

	if err == nil {
		t.Fatalf("期望因客户端断开返回取消错误，但得到了 nil")
	}
	if count >= 10 {
		t.Fatalf("客户端断开后未能及时中止，仍然读取了全部 %d 条数据", count)
	}
	t.Logf("✅ 客户端断开即时中断检验通过，在第 %d 条事件后成功止损退出 (err: %v)", count, err)
}

// TestForwardApproveSSE 测试 4：检验人工审批接口 JSON 序列化字段 (approved) 与恢复流读取
func TestForwardApproveSSE(t *testing.T) {
	mockPythonServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/approve" {
			t.Errorf("期望请求路径 /approve，实际得到: %s", r.URL.Path)
		}
		var rawMap map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&rawMap)

		// 严格验证 JSON 字段名必须为 "approved"，与 Python FastAPI 对齐
		approvedVal, exists := rawMap["approved"]
		if !exists || approvedVal != true {
			t.Errorf("JSON 字段校验失败！Python 端要求字段名为 'approved': true，实际收到: %v", rawMap)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"node\": \"approval\", \"status\": \"COMPLETED\"}\n\n")
		fmt.Fprintf(w, "data: [DONE]\n\n")
	}))
	defer mockPythonServer.Close()

	agentCli := client.NewAgentClient(mockPythonServer.URL)
	var lines []string

	err := agentCli.ForwardApprove(context.Background(), &client.ApproveTaskRequest{
		ThreadID: "thread_danger_99",
		Approved: true,
	}, func(line string) error {
		lines = append(lines, line)
		return nil
	})

	if err != nil {
		t.Fatalf("ForwardApprove 执行出错: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("期望收到 2 行恢复流事件，实际收到: %v", lines)
	}
	t.Logf("✅ 人工审批恢复流与字段对齐检验通过: %v", lines)
}
