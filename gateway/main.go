package main

// Go 高性能接入层入口
import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"gateway/client"
	"gateway/middleware"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	//1.初始化Redis 客户端连接
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})
	//2. 初始化 Python Agent 客户端连接
	agentBaseURL := os.Getenv("AGENT_URL")
	if agentBaseURL == "" {
		agentBaseURL = "http://localhost:8000"
	}
	agentCli := client.NewAgentClient(agentBaseURL)
	//3. 创建Gin Web 引擎
	r := gin.Default()
	//4. 挂载全局令牌桶防刷限流中间件（每分钟每个IP最多30次请求）
	r.Use(middleware.RedisTokenBucketLimiter(rdb, 30, time.Minute))
	//健康检查探针(K8s/Docker 存活检测)
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"server": "Go Gateway",
		})
	})
	//5. 核心流式代理端点： POST/api/v1/agent/stream
	r.POST("/api/v1/agent/stream", func(c *gin.Context) {
		var req client.StreamTaskRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "请求参数不规范: " + err.Error(),
			})
			return
		}
		if req.ThreadID == "" {
			req.ThreadID = fmt.Sprintf("session_%d",
				time.Now().UnixNano())
		}
		//A. 设置标准SSE响应头，告知浏览器这是一个分块长连接流
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("Transfer-Encoding", "chunked")
		// 检查底层ResponseWriter是否支持Flush强制冲刷
		flusher, ok := c.Writer.(http.Flusher)
		if !ok {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "不支持Flush强制冲刷",
			})
			return
		}
		//B. 核心高并发控制：捕获客户端是否意外断开连接
		clientClosed := c.Request.Context().Done()
		//创建带超时的级联Context(最多允许执行120秒，避免Agent死循环挂死连接)
		ctx, cancel := context.WithTimeout(c.Request.Context(),
			120*time.Second)
		defer cancel()
		fmt.Printf("🚀 [Go Gateway] 收到新任务 (ThreadID: %s), 开启 SSE 实时转发...\n", req.ThreadID)
		//C. 转发给Python Agent 并实时Flush到客户端
		err := agentCli.ForwardStream(ctx, &req, func(line string) error {
			select {
			case <-clientClosed:
				//客户端断开(如用户关了浏览器标签页)
				fmt.Println("⚠️ [Go Gateway 警报] 客户端已断开，立即终止向下游推流以节省 Token！")
				return fmt.Errorf("client closed")
			default:
				//将数据写入客户端响应体并Flush
				fmt.Fprintf(c.Writer, "%s\n\n", line)
				flusher.Flush()
				return nil
			}
		})
		if err != nil && err.Error() != "client closed" {
			fmt.Printf("❌ [Go Gateway] 转发过程出错: %v\n", err)
			fmt.Fprintf(c.Writer, "data: {\"error\": \"%s\"}\n\n", err.Error())
			flusher.Flush()
		}
		//推送结束标志
		fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
		flusher.Flush()
		fmt.Printf("✅ [Go Gateway] 任务 (ThreadID: %s) 已完成，已推送所有数据到客户端\n", req.ThreadID)
	})
	//6. 人工审批恢复端点：POST/api/v1/agent/approve
	r.POST("/api/v1/agent/approve", func(c *gin.Context) {
		var req client.ApproveTaskRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "请求参数不规范: " + err.Error(),
			})
			return
		}
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		flusher, _ := c.Writer.(http.Flusher)
		fmt.Printf("🚀 [Go Gateway] 收到新审批任务 (ThreadID: %s，Approved: %v), 开启 SSE 实时转发...\n", req.ThreadID, req.Approved)
		_ = agentCli.ForwardApprove(c.Request.Context(), &req, func(line string) error {
			fmt.Fprintf(c.Writer, "%s\n\n", line)
			flusher.Flush()
			return nil
		})
		fmt.Fprintf(c.Writer, "data: [DONE]\n\n")
		flusher.Flush()
	})
	//7. 启动HTTP监听于8080端口
	port := ":8080"
	fmt.Printf("🚀 [Go Gateway] 启动监听端口: %s\n", port)
	if err := r.Run(port); err != nil {
		panic(err)
	}
}
