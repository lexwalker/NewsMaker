# Расписание прогонов (Windows Task Scheduler)

Пайплайн запускается **3 раза в день по МСК (UTC+3)**:
9:00 / 13:00 / 16:00. Каждый прогон забирает новости, опубликованные с
момента предыдущего прогона минус 20-минутный нахлёст — это страхует
от пропуска новостей, попавших на границу окна.

## Как работает окно

`scripts/batch_fetch_test.py` сам вычисляет временное окно при старте:

```
since = max(last_run_at − 20 минут, now − 24 часа)
```

— `last_run_at` лежит в `data/state.json` и обновляется в конце
  каждого успешного прогона
- если файла нет (первый запуск) или предыдущий прогон был >24 ч назад
  (бот стоял), окно ограничивается ‑24 ч — чтобы не тащить
  многомесячный бэклог

Перекрытие безопасно: антидуп (по точному и фази-совпадению заголовка
\+ бренду) не плодит дубли, а только дописывает новые URL в существующий
кластер.

Все параметры окна — флаги CLI:

| Флаг | По умолчанию | Описание |
|---|---|---|
| `--since-overlap-minutes` | 20 | На сколько минут до `last_run_at` отступить вглубь |
| `--max-lookback-hours` | 24 | Потолок окна (на холодном старте / после простоя) |
| `--no-window` | off | Выключает режим расписания: использует `FRESHNESS_HOURS` и НЕ обновляет `state.json` (для ad-hoc прогонов) |
| `--state-path` | `data/state.json` | Где хранить `last_run_at` |
| `--runs-log` | `data/runs.log` | JSONL-журнал прогонов |

## Установка трёх задач в Task Scheduler

Все три задачи отличаются только временем — действие одинаковое.

### Через PowerShell (рекомендуется)

Открой PowerShell **от администратора** в корне проекта и выполни:

```powershell
$Project = "C:\Users\Defau\OneDrive\Desktop\NewsMaker"
$PyExe   = "C:\Users\Defau\AppData\Local\Programs\Python\Python312\python.exe"
$Script  = "$Project\scripts\batch_fetch_test.py"

foreach ($t in @("09:00", "13:00", "16:00")) {
    $taskName = "NewsMaker_$($t.Replace(':',''))"
    $action   = New-ScheduledTaskAction `
        -Execute $PyExe `
        -Argument "`"$Script`"" `
        -WorkingDirectory $Project
    $trigger  = New-ScheduledTaskTrigger -Daily -At $t
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -DontStopIfGoingOnBatteries `
        -AllowStartIfOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 2)
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description "NewsMaker batch fetch — scheduled run at $t MSK" `
        -Force
}
Get-ScheduledTask -TaskName "NewsMaker_*" | Select-Object TaskName, State, NextRunTime
```

Параметры:
- **`-StartWhenAvailable`** — если ноут спал в момент триггера, прогон
  стартует сразу как машина проснётся (`max_lookback_hours=24` спасёт
  от потерянного окна).
- **`-DontStopIfGoingOnBatteries`** — не останавливать прогон, если
  ноут перешёл на батарею в середине.
- **`-ExecutionTimeLimit 2h`** — страховка от зависшего прогона.

### Через GUI (если PowerShell недоступен)

1. `taskschd.msc` → Create Basic Task
2. Name: `NewsMaker_0900` (и так для 1300, 1600)
3. Trigger: Daily, время 09:00 / 13:00 / 16:00
4. Action: Start a program
   - Program: `C:\Users\Defau\AppData\Local\Programs\Python\Python312\python.exe`
   - Arguments: `scripts\batch_fetch_test.py`
   - Start in: `C:\Users\Defau\OneDrive\Desktop\NewsMaker`
5. Properties → General → ✅ "Run whether user is logged on or not"
6. Properties → Settings → ✅ "Start the task as soon as possible after a scheduled start is missed"
7. Properties → Conditions → снять ✅ "Start the task only if the computer is on AC power"

## Удалить расписание

```powershell
Get-ScheduledTask -TaskName "NewsMaker_*" | Unregister-ScheduledTask -Confirm:$false
```

## Проверка после установки

```powershell
# Проверить расписание
Get-ScheduledTask -TaskName "NewsMaker_*" | Select-Object TaskName, State, NextRunTime

# Запустить вручную (как будто триггер сработал) — для проверки
Start-ScheduledTask -TaskName "NewsMaker_0900"

# Журнал прогонов (1 строка JSON на прогон)
Get-Content data\runs.log -Tail 5

# Текущее состояние
Get-Content data\state.json
```

`data/runs.log` пишется в формате JSONL — по одной записи на прогон:

```json
{"run_at": "2026-04-28T13:00:42+00:00", "status": "ok",
 "window_since": "2026-04-28T08:40:00+00:00", "rows_total": 87,
 "rows_new": 12, "cost_usd": 0.41, "elapsed_s": 412.5,
 "window_fallback": false, "previous_run_at": "2026-04-28T09:00:00+00:00"}
```

Через неделю это даст статистику по которой можно решать:
объединять ли 13:00/16:00, увеличивать `--since-overlap-minutes`, и т.п.

## Что мерять первую неделю

1. **`window_fallback: true`** — если часто, значит ноут спит / задачи
   не срабатывают. Проверить настройки энергосбережения.
2. **`rows_new` за прогон** — если 13:00 и 16:00 регулярно дают <5,
   объединить с одним прогоном в 14:00.
3. **`cost_usd` суммарно за день** — должно быть ~$2; если выше —
   разобраться, не сбросился ли SQLite-кэш.
4. **Случаи разрыва кластеров** — новость пришла в 13:00 одной строкой
   и в 16:00 ещё одной (антидуп не сработал). Это сигнал расширить
   `similarity_threshold` в `detect_earliest_in_corpus`.
