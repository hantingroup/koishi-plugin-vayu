import type { Channel, Context, h, Tables } from 'koishi'
import {} from '@koishijs/plugin-help'
import { Jieba } from '@node-rs/jieba'
import { dict } from '@node-rs/jieba/dict'
import { $, Logger, Schema, sleep, Time } from 'koishi'
import { shortcut, stream } from 'koishi-plugin-montmorill'
import { mergeChunks } from './algorithm'

export const name = 'vayu'
const logger = new Logger(name)

export interface Config {
  dataUrl: string
  interval: number
  maxChunks: number
  punctBias: number
}

export const Config: Schema<Config> = Schema.object({
  dataUrl: Schema.string().role('url').description('随蓝题库URL。').default('https://raw.githubusercontent.com/HanTingQuan/HTDictionary/refs/heads/main/vayu.csv'),
  interval: Schema.number().default(3 * Time.second).role('ms').description('间隔时间。'),
  maxChunks: Schema.number().default(5).description('最大分句数。'),
  punctBias: Schema.number().min(0).step(0.05).default(0.7).max(2).role('slider').description('标点偏好系数，小于1时鼓励在标点后断句，大于1时抑制。'),
})

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

export const inject = ['database']

const SPACE = /\s+/

export async function apply(ctx: Context, config: Config) {
  ctx.model.extend('vayu', {
    id: 'unsigned',
    vayu: 'char',
    source: 'string',
    answer: 'string',
    desc: 'string',
  }, { primary: 'id' })

  const jieba = Jieba.withDict(dict)
  const streaming = new Map<Channel['id'], number>()

  ctx.command('vayu [id:number]', '从随蓝题库中出题。')
    .alias('随蓝', '📘来一道随蓝')
    .option('interval', '-i <interval:number> 间隔时间（秒）。')
    .option('answer', '-a 查看答案。')
    .option('bias', '-b <bias:number> 标点偏好系数。', { hidden: true })
    .action(async ({ options, session }, id?: number) => {
      if (!session)
        return

      const [vayu] = await ctx.database.select('vayu', id ? { id } : {})
        .orderBy($.random)
        .limit(1)
        .execute()
      if (!vayu)
        return '未找到符合条件的随蓝！'

      if (options?.answer)
        return `${vayu.source}#${vayu.id}${vayu.vayu}\n${vayu.answer}\n${vayu.desc}`

      const description = vayu.desc.trim()
      const words = description.startsWith('1.')
        ? description.split(SPACE).map(word => `${word} `)
        : jieba.cut(description)

      const chunks = mergeChunks(words, config.maxChunks, options?.bias ?? config.punctBias)
      const interval = (options?.interval || 0) * 1000 || config.interval

      async function* generator(isDirect: boolean) {
        for (let index = 0; index < chunks.length; index++) {
          const chunk = chunks[index]

          if (index === chunks.length - 1) {
            return [
              `${chunk}我读完了。`,
              `> 回答随蓝 👉 ${shortcut.input(`/vayu.answer ${vayu.id} `)}`,
              `> 查看答案 👉 ${shortcut(isDirect, `/vayu ${vayu.id} -a`)}`,
              `> 再来一题 👉 ${shortcut(isDirect, '/vayu')}`,
            ].join('\n')
          }

          // eslint-disable-next-line style/multiline-ternary
          yield index === 0 ? [
            vayu.source,
            shortcut.input(`/vayu.answer ${vayu.id} `, `#${vayu.id}`),
            vayu.vayu,
            `\n${chunk}`,
          ].join('') : chunk

          await sleep(interval)
        }
      }

      streaming.set(session.channelId!, vayu.id)
      const send = (element: h): Promise<[string]> => {
        return streaming.get(session.channelId!)
          ? session.send(element) as Promise<[string]>
          : Promise.resolve([''])
      }

      await stream(generator(session.isDirect), send)
    })
    .subcommand('.answer <id:number> <answer:string>', '回答随蓝')
    .action(async ({ session }, id, answer) => {
      if (!session)
        return

      const [vayu] = await ctx.database.get('vayu', { id })
      if (!vayu)
        return '未找到符合条件的随蓝！'
      const correctAnswer = vayu.answer.split('/')
      if (!correctAnswer.includes(answer))
        return '❌️回答错误！'
      session.execute('vayu')
      const vayuId = streaming.get(session.channelId!)
      if (vayuId === id)
        streaming.delete(session.channelId!)
      return '✅️回答正确！'
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
