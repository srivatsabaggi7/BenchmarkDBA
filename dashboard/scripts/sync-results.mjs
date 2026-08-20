import { copyFileSync, existsSync, mkdirSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const dashboardRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const source = resolve(dashboardRoot, '..', 'benchmark_results.json')
const destination = resolve(dashboardRoot, 'public', 'results.json')

if (!existsSync(source)) {
  throw new Error(`Benchmark results file not found: ${source}`)
}

mkdirSync(dirname(destination), { recursive: true })
copyFileSync(source, destination)
console.log(`Synchronized ${source} -> ${destination}`)