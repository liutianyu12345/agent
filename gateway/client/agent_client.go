package client

// 面向 Python Agent 服务的跨语言调用客户端
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// AgentClient 面向python agent 服务的跨语言通信客户端
type AgentClient struct {
	BaseURL    string       // Agent 服务的 Base URL，例如：http://localhost:8000
	HTTPClient *http.Client // HTTP 客户端实例，用于发送请求和接收响应
}

// NewAgentClient 创建客户端实例
func NewAgentClient(baseURL string) *AgentClient {
	return &AgentClient{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			// 长连接 SSE 流式请求不能设置全局短超时，超时通过 context 控制
			Timeout: 0,
		},
	}
}

// StreamTaskRequest发往python服务的任务结构体
type StreamTaskRequest struct {
	Prompt   string `json:"prompt"`
	ThreadID string `json:"thread_id"`
}

// ApproveTaskRequest 发往python服务的审批结果结构体
type ApproveTaskRequest struct {
	ThreadID string `json:"thread_id"`
	Approve  bool   `json:"approve"`
}

// ForwardStream 将任务请求发给 Python Agent，并把逐行收到的 SSE 数据写入 onChunk 回调
func (ac *AgentClient) ForwardStream(ctx context.Context, req *StreamTaskRequest,
	onChunk func(line string) error) {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}
	//创建带Context的HTTP请求(若客户端断开，可立刻取消请求)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", ac.BaseURL+"/stream", bytes.NewReader(reqBytes))
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := ac.HTTPClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("请求失败: %s, 响应体: %s", resp.Status, string(body))
	}
	//使用 bufio.Reader逐行读取 Python 端吐出的SSE数据
	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-ctx.Done():
			//上层传递了取消信号（如用户关了网页）
			return ctx.Err()
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					//响应体读取完成
					return nil
				}
				return fmt.Errorf("读取响应体失败: %w", err)
			}
			trimmed := strings.TrimRight(line, "\r\n")
			if trimmed != "" {
				if err := onChunk(trimmed); err != nil {
					return err
				}
			}
		}
	}
}
