import {Redis, type RedisOptions} from "ioredis"

export interface JobOptions {
    attempts?: number;
    delay?: number;
    backoff?: number;
    lifo?: boolean;
    timestamp?: number;
    sizeLimit?: number;
    jobId?: string;
    priority?: number;
}

const MAX_SEQUENCE = 2 ** 32 - 1;
const PRIORITY_MULTIPLIER = 2 ** 32;

export class Queue<T=unknown> {
    readonly redis: Redis;

    constructor(
        public readonly name: string,
        connection: RedisOptions = {}
    ) {
        this.redis = new Redis(connection);
    }

    key(part: string) {
        return `whalemq:${this.name}:${part}`;
    }

    async checkKey(jobId: string): Promise<boolean> {
        return (await this.redis.exists(this.key(`job:${jobId}`))) === 1;
    }

    private calculateScore(lifo: boolean, priority: number, sequence: number) {
        const order = lifo ? MAX_SEQUENCE - Number(sequence) : Number(sequence);
        const score = priority * PRIORITY_MULTIPLIER + order; 
        return score
    }

    async add(name: string, data: T, opts: JobOptions = {}) {
        if(opts.sizeLimit !== undefined && Buffer.byteLength(JSON.stringify(data), "utf-8") > opts.sizeLimit) {
            throw new Error(
                `Job data exceeds size limit of ${opts.sizeLimit} bytes`
            );
        }

        const priority = opts.priority ?? 0;

        if(!Number.isInteger(priority) || priority < 0 || priority > MAX_SEQUENCE) {
            throw new Error("Priority must be a non-negative integer");
        }

        const id = opts.jobId ?? await this.redis.incr(this.key("sequence"));
        
        if (Number(id) > MAX_SEQUENCE) {
            throw new Error("Queue sequence limit exceeded");
        }

        const time = opts.timestamp ?? Date.now();

        if(opts.jobId) {
            const exists = await this.checkKey(String(id));

            if(exists) {
                throw new Error(
                    `Job Id already exists in the queue: ${opts.jobId}`
                )
            }
        }

        const score = this.calculateScore(opts.lifo ? true : false, priority, Number(id));

        await this.redis.hset(
            this.key(`job:${id}`),{
            id: id,
            data: JSON.stringify(data),
            attempts: 0,
            maxAttempts: opts.attempts ?? 1,
            backoff: opts.backoff ?? 1000,
            state: opts.delay ? "delayed" : "waiting",
            timestamp: time
        });

        if(opts.delay !== undefined && opts.delay < 0) {
            await this.redis.zadd(this.key("delayed"), Date.now() + opts.delay, id);
        } else {
            await this.redis.zadd(
                this.key("prioritized"),
                score,
                id
            );
        }

        return id;
    }

    async getJob(id: string) {
        const job = await this.redis.hgetall(this.key(`job:${id}`));
        return job.id ? job : null; 
    }

    async getCounts() {
        const [prioritized, active, delayed, completed, failed] = await Promise.all([
            this.redis.llen(this.key("prioritized")),
            this.redis.llen(this.key("active")),
            this.redis.zcard(this.key("delayed")),
            this.redis.llen(this.key("completed")),
            this.redis.llen(this.key("failed")),
        ])

        return { prioritized, active, delayed, completed, failed };
    }

    
    
    async close() {
        await this.redis.quit()
    }
}