# 云图历史 v4：从 748f9d3 重建

日期：2026-11-22（实施）。用户裁决：v1–v3 全部作废（未提交改动全部丢弃），从提交 748f9d3 重建，按最简方案快速迭代，交付前由助手用 computer-use 真机验收。

## 为什么推倒

v1–v3（全部未提交）在引擎云块累积了 14 个相互作用的可变旗标（wanted/neighbor/prefetched/retry/floor/floor_armed/stopped_floor/unavailable/refresh_pressed/serve_sent/repost/requested/…），每轮修复引入新的交互 bug（frame_settled 短路两次重蹈、miss 自相毒化、种子毒化、状态行过期）。真机日志证实机制"能跑通"但体验失败：

| 症状 | 日志证据（2026-11-22 12:33–12:57 会话） | v4 的结构性解法 |
|---|---|---|
| 新拖动不抢占在途抓取（多等 ~35s） | 拖到 8-16 时 worker 还在抓 8-22 的预取场，用户等了 04:55:21→04:56:11 | worker 一次一个 attempt，独立线程；新请求到达即置其取消标志、丢弃其结果通道并立刻开始新 attempt（`cloud_worker.rs::coordinator`） |
| Stop 无法中断 50s 下载 | 只能在"最近检查点"退出 | 取消标志在每个读块之间与驻停切片内检查；引擎侧 Stop 立即置 stopped 态 |
| 未归档时段无解释空白 | 05:05–08:53 全部无云、无提示 | 场次行（CloudRound）每 tick 由时间线状态重算派生——`fetching round X UTC…` / `has not been published yet` / `is not held by the upstream` / `stopped` |
| 状态卡在旧场次 | 场次行/状态行显示 09:00 而用户拖到 03:00 | 场次行是状态机的直接读数，无第二条消息流可过期 |
| 每停一处等 55 秒 | NSMC compose ~50s 服务端渲染 | 归档累积（live 轮询入档 + 落点打钉），重访零网络；停手去抖避免连续取消 |

## 设计（最简）

**归档** `assets/cloud_archive.rs`：manifest.toml v2（stamp/raw/derived/pinned），格式与 v3 磁盘数据兼容，用户已有 18 场直接可用。操作仅 6 个：record / pin / prune（保留期跳 pinned；预算先清未钉）/ rounds()（内存索引）/ load_jpeg / raw_files。种子：来源 meta 里的真实场次（无 mtime 冒充）。

**来源** `CloudSource` 增两个方法：`fetch_round_at(round) -> Ok(Some)/Ok(None)/Err`（None=上游列表没有该场）与 `settled_round()`（种子用）。NSMC 用宽列表（hourRange=10000，会话内持有；miss 不失效列表）+ 现有 compose 端点；EUMETSAT 用现有瓦片管线按时间请求。派生走"只派生不落盘"内核（derive_encode / process_map_only），历史场不覆写 live 缓存。

**时间线** `engine::cloud_timeline`（纯函数：floor_for / covering_round / round_text）+ 引擎 7 个记忆字段（in_flight / settle / unavailable / stopped / retry_at / forced / round_line_sent）。tick 每拍：消费结果 → live 则 gate=Live → 固定则算覆盖 → 有覆盖则 gate=Round 并在 drawn≠round 时派发（归档秒答）→ 无覆盖则按策略 + 停手 1s（CLOUD_SETTLE）+ 重试窗派发 fetch。Gate（`renderer::CloudGate`）拒绝会话不再穿着的帧——被抛弃的旧抓取落地也不上屏。

**可见性** `clouds_visible_at(at)`：固定时刻只穿覆盖场；无覆盖不穿云。无场次来源（Remote）任何时刻穿当前图。

**worker** `CloudRequest::{Poll, Fetch}`：Poll 受引擎 busy 守卫（不抢占）；Fetch 即发即走（新落点抢占旧抓取）。结果经 `take_fetch_result()` 三态回引擎。

**UI**：场次行（按钮行下，CloudRound 读数）；Fetch this moment（立即按落点取图，busy 时禁用）；Stop（仅 busy 出现）；四设置项 Keep history / Cache imagery / Archive size limit / Missing imagery（三档），选中即生效 + 持久化。

## 明确不做（最简边界）

后台预取/并发探测、最近场穿图（保持 v2 的严格绑定）、数据库、正式状态机枚举、LRU 服务缓存、EUMETSAT 归档行的 identity 校验（identity 只存不比，不匹配的服务回落上游抓取）。

## 测试

- 归档 7 个（round-trip / 替换 / raw-only / 保留 / 预算 / current / 内存态）。
- 时间线纯函数 8 个（floor 三态、覆盖边界、空档、未来窗）。
- worker 3 个（切换代际、Stop 中断后干净恢复、**新抓取抢占在途**——本轮核心的回归证明）。
- 引擎集成 8 个（`tests/engine/clouds_history.rs`：归档秒答、停手抓取+打钉、**抢占**、Stop 站下与恢复、上游不持有不重试、两档策略、未来场次）。

## 已修的实现 bug（记录）

- coordinator 在 attempt 结束后未清 busy → 首次 poll 后引擎永远拿不到空闲（测试挂起暴露）。
- 无场次来源在固定时刻被隐藏 → 破坏 Remote 与全部 fixture 测试；改为与 live 同语义。
- 归档 State 默认 keep_derived 应与 CloudCacheMode::default() 一致（true）。
- 测试时间戳带纳秒 vs 归档整秒比较（stamp 截断）——测试侧统一整秒。

## 验证（已全部通过）

- **全量 workspace 测试两轮全绿**（约 2,180 个；含 19 个新测试：归档 7、时间线纯函数 8、worker 3、引擎集成 8——归档秒答 / 停手抓取+打钉 / **抢占** / Stop 站下与恢复 / 上游不持有不重试 / 两档策略 / 未来场次）。
- **clippy 全部清零**（workspace, all-targets）。
- **release 构建 + `cargo xtask bundle --verify`**：90 文件读回一致 + 冒烟渲染。
- **真机验收**（dev 构建接真实 NSMC 服务，临时缓存目录隔离，配置驱动落点——与拖动落点同路径）：

| 阶段 | 场景 | 结果（日志证据） |
|---|---|---|
| A | 固定时刻 11-22 04:53（归档空） | 停手派发 → compose → `entered the cloud archive pin=true` → 上屏 |
| B | 固定时刻 04:20（归档已有 04:00） | `the archive serves the round` 250ms 秒答、零网络 → 上屏；启动瞬态（上一会话 live 帧误穿 1.5s）当场发现并修复（gate 默认 Nothing） |
| C | 恢复 live | 穿最新场 07:00，轮询确认 still current |
| D | **8-16 09:45（用户原始失败场景）** | 停手派发 → 宽列表 9942 条持有 → compose 57.2s → **打钉入档** → 上屏，8 月云图穿着正确 |

- 真机归档最终状态：三个场，两个 fetched（pinned）+ 一个 live（未钉）——pinned/live 语义按设计工作。
- 已知验收限制：本会话的合成鼠标点击无法触达 Slint 面板的展开箭头与时间条拖动（事件位置未被 Slint 命中），落点改由 config scene_time 驱动——与 `UpdateParams` 落点完全同路径；拖动 UI 管线自 HEAD 未改动且有引擎级测试覆盖（含抢占与 Stop）。UI 场次行/按钮/四个设置项的布局存在（构建通过 + Slint UI 测试通过），鼠标层面的点检留待用户实测。

## 验收口径（给用户）

1. 启动后拖时间条到 8 月：停手约 1 秒后场次行变 `fetching round 2026-08-16 09:00 UTC…`，约 1 分钟后变 `… (historical)`，屏幕出现 8 月云图。
2. 已归档时刻再拖回去：云图**立即**出现（`historical`，无网络等待）。
3. 抓取进行中拖到别处：旧的立刻放弃，新的开始抓（不等 50 秒）。
4. 抓取中点 Stop：立即中断，场次行 `stopped — drag again or press Fetch this moment`；再拖动或按 **Fetch this moment** 恢复。
5. 场次行每 tick 由引擎状态重算——它说"fetching"就是在抓，说"historical"就是穿着，不会卡在旧话术。
