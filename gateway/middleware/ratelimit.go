package middleware

// Redis 令牌桶限流中间件
import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RedisTokenBucketLimiter 令牌桶/固定窗口限流中间件
// 参数说明：
//   - rdb: Redis 客户端实例
//   - maxTokens: 窗口周期内允许的最大请求次数（例如 10 次）
//   - window: 计数窗口时间周期（例如 1 分钟）
func RedisTokenBucketLimiter(rdb *redis.Client, maxTokens int64, window time.Duration) gin.HandlerFunc {

	return func(c *gin.Context) {
		//获取调用者唯一标识
		tenantID := c.GetHeader("X-Tenant-ID")
		if tenantID == "" {
			tenantID = c.ClientIP()
		}
		ctx := context.Background()
		bucketKey := "limit_bucket:" + tenantID
		// 2. 利用 Redis 的原子操作 INCR 递增访问次数
		// 注意：INCR 是单线程原子执行的，天然具备高并发安全性，不会出现并发计数错误
		currCount, err := rdb.Incr(ctx, bucketKey).Result()
		if err != nil {
			// 【工业级高可用设计：降级放行】
			// 当 Redis 宕机或网络闪断时，千万不能让正常业务直接全部瘫痪；
			// 此时记录日志并放行请求（Fail-open），保障核心可用性。
			c.Next()
			return
		}
		// 3. 如果是当前时间窗口内的第一次访问（计数值刚好为 1），设置该 Key 的过期时间
		if currCount == 1 {
			rdb.Expire(ctx, bucketKey, window)
		}
		//4.判断是否超出配额限制
		if currCount > maxTokens {
			// 超额拦截，返回 HTTP 429 Too Many Requests
			c.JSON(http.StatusTooManyRequests, gin.H{
				"code":      429,
				"message":   "请求频率超过限制，请稍后重试",
				"tenant_id": tenantID,
			})
			//终止后续处理
			c.Abort()
			return
		}
		// 5. 没有超出配额限制，继续处理请求
		c.Next()
	}
}
