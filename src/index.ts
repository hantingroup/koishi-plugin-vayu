import type { Channel, Context, Tables } from 'koishi'
import {} from '@koishijs/plugin-help'
import { $, h, Logger, Schema, sleep, Time } from 'koishi'
import {} from 'koishi-plugin-jieba'
import { mergeChunks } from './algorithm'

export const name = 'vayu'
const logger = new Logger(name)

export interface Config {
  dataUrl: string
  interval: number
  maxChunks: number
  punctBias: number
}

export const Config: Schema<Config> = Schema.intersect([
  Schema.object({
    dataUrl: Schema.string().role('link').description('题库 URL。').default('https://raw.githubusercontent.com/HanTingQuan/HTDictionary/refs/heads/main/vayu.csv'),
    interval: Schema.number().default(3 * Time.second).role('ms').description('间隔时间。'),
    maxChunks: Schema.number().default(5).description('最大分句数。'),
  }),
  Schema.object({
    punctBias: Schema.number().min(0).step(0.01).default(0.7).max(1).role('slider').description('标点偏好系数，小于1时鼓励在标点处断句，大于1时抑制。'),
  }).description('高级设置'),
])

declare module 'koishi' {
  interface Tables {
    vayu: {
      id: number
      vayu: string
      source: string
      answer: string
      desc: string
    }
  }
}

export const inject = ['database', 'jieba']

const SPACE = /\s+/

export async function apply(ctx: Context, config: Config) {
  ctx.model.extend('vayu', {
    id: 'unsigned',
    vayu: 'char',
    source: 'string',
    answer: 'string',
    desc: 'string',
  }, { primary: 'id' })

  const streaming = new Map<Channel['id'], number>()

  ctx.command('vayu [id:number]', '从随蓝题库中出题')
    .option('interval', '-i <interval:number> 间隔时间（秒）')
    .option('answer', '-a 查看答案')
    .option('bias', '-b <bias:number> 标点偏好系数', { hidden: true })
    .action(async ({ options, session }, vayuId?: number) => {
      if (!session)
        return

      const [vayu] = await ctx.database.select('vayu', vayuId ? { id: vayuId } : {})
        .orderBy($.random)
        .limit(1)
        .execute()
      if (!vayu)
        return await session.send('未找到符合条件的随蓝！')

      if (options?.answer)
        return h.text(`${vayu.source}#${vayu.id}${vayu.vayu}\n${vayu.answer}\n${vayu.desc}`)

      const description = vayu.desc.trim()
      const words = description.startsWith('1.')
        ? description.split(SPACE).map(word => `${word} `)
        : ctx.jieba.cut(description)

      const chunks = mergeChunks(words, config.maxChunks, options?.bias ?? config.punctBias)
      const interval = (options?.interval || 0) * 1000 || config.interval

      await session.send(h(
        'stream',
        vayu.source,
        h('inlinecmd', { text: `vayu.answer ${vayu.id}` }, `#${vayu.id}`),
        `${vayu.vayu}\n`,
      ))

      for (let index = 0; index < chunks.length; index++) {
        const chunk = chunks[index]
        await session.send(h('stream', chunk))
        if (index === chunks.length - 1)
          return h('stream', { done: true }, '我读完了。')
        await sleep(interval)
      }
    })
    .subcommand('.answer <id:number> <answer:string>', '回答随蓝')
    .action(async ({ session }, id, answer) => {
      if (!session)
        return
      const [vayu] = await ctx.database.get('vayu', { id })
      if (!vayu)
        return await session.send('未找到符合条件的随蓝！')
      const correctAnswer = vayu.answer.split('/')
      if (!correctAnswer.includes(answer))
        return await session.send('❌️回答错误！')
      const vayuId = streaming.get(session.channelId!)
      if (vayuId === id)
        streaming.delete(session.channelId!)
      await session.send('✅️回答正确！')
      await session.execute('vayu')
    })

  const stats = await ctx.database.stats()
  if (!stats.tables.vayu?.count) {
    logger.info('随蓝题库为空，下载中...')
    const parser = (await import('csv-parse')).parse({ columns: true })
    const buffer: Tables['vayu'][] = []
    parser.on('readable', () => {
      let record = parser.read()
      while (record !== null) {
        buffer.push(record)
        record = parser.read()
      }
    })
    parser.write(await ctx.http.get(config.dataUrl))
    parser.end(() => {
      ctx.database.upsert('vayu', buffer)
      logger.info(`随蓝题库下载完成，共 ${buffer.length} 条记录。`)
    })
  }
}
