docker pull redis

mkdir -p /home/redis/conf /home/redis/data
touch /home/redis/conf/redis.conf

docker run -d \
  --name my-redis \
  -p 6379:6379 \
  -v /Users/evaseemefly/01env/conf/redis/conf/redis.conf:/etc/redis/redis.conf \
  -v /Users/evaseemefly/03data/09envs/redis/data:/data \
  redis \
  redis-server /etc/redis/redis.conf --appendonly yes
