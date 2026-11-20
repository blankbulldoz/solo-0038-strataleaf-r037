# 恢复与观测

备份必须调用 archives.backup，不能只拷贝活动主文件。backup 与 restore 都不覆盖存在目标；恢复使用新路径，先校验来源 SHA-256，再打开新句柄。主库恢复生成新身份，副本恢复绑定来源。恢复清理原机保留点与消费者；消费方重新注册时须根据已恢复的 floor/head 判断是否需要重建。

## 接收环境原地切换

`Archives.restore` 保留“直接恢复到全新文件”的用法；已有接收环境改用备份时使用可重开的切换入口，避免手工换目录。切换在目标数据库旁的 `<目标名>.switch/` 目录内完成，状态机为 `preparing -> prepared -> committing -> committed`，全部状态写入 `state.json`（原子替换），`lock` 上的 `flock` 串行化同目标的切换尝试，进程退出时内核自动释放锁，不会留下僵死锁。

- `Archives.switch_prepare(archive, target, expected_sha256=None, follower=False)`：校验归档摘要，把归档在线备份到 `staged.db`，再以只读不可变连接完成读验——SQLite `integrity_check`、外键检查、必需元数据、提交连续性、模式/身份安装结果，以及与归档逐文档逻辑指纹比对。此阶段不触碰目标数据库，调用方继续使用旧库；任何失败都只留下 staging，旧环境字节不变。重复调用是可重开入口：沿用同一状态重建 staged 并累加 `attempts`。
- `Archives.switch_commit(target, keep_retired=False)`：仅在 `prepared` 后可提交。提交前先做打开句柄守卫：本进程登记的 Storage 句柄、`/proc` 扫描到的其他进程 fd，以及 `/proc/locks` 中仍作用在主库/WAL/SHM 上的 POSIX/FLOCK 锁任一存在即抛 `Conflict`，绝不覆盖仍被打开的 SQLite 文件；平台缺少 `/proc` 时失败关闭而不是放行。守卫通过后把旧的主库及 `-wal`/`-shm` 同目录 rename 到 `retired/`，再把 staged rename 到目标路径并 fsync 目录，最后落 `committed` 状态。
- `Archives.switch(archive, target, ...)`：一次性入口，等价于 prepare 后 commit，可在任意阶段重复调用以重开切换。
- `Archives.switch_abort(target)`：取消未提交的切换并删除 staging；已提交的切换拒绝 abort。prepare 失败后可用它回到 `absent`。
- `Archives.switch_cleanup(target)`：仅允许清理已 `committed` 的 staging；开始另一个不同归档的切换前必须先清理或 abort。
- `Archives.switch_status(target)`：只读查看持久化阶段、staged 是否存在、retired 文件清单。

进程在切换中退出时，重开 `switch_commit` 自动恢复：rename 全部完成只丢状态的，按目标指纹确认后补记 `committed`；旧文件已移走而 staged 未安装的，继续完成安装；两边都不完整但 `retired/` 保有完整旧库的，先回滚旧库再重建 staged。恢复路径不再重复打开句柄守卫，因为授权在退出前已经完成。

清理规则：默认 `keep_retired=False`，提交完成后删除 `retired/` 与整个 staging 目录；`keep_retired=True` 保留旧库文件并在状态中列出路径，直到显式 `switch_cleanup` 或下一次成功切换。staging 中只允许出现 `lock`、`state.json`、`staged.db`、`retired/` 及原子写临时文件，临时文件在每次取得锁时清扫。

切换后的归属：切换是环境级操作，提交成功后旧路径上的文件已是新 inode，进程内旧 Storage 句柄保持关闭态语义（提交守卫保证提交时本进程无打开句柄）；未被守卫覆盖到的外部旧句柄仍绑定被移走的旧 inode，不应继续用于服务。主库切换轮换 `database_id` 与 `cursor_secret`，旧签名游标在新库上明确抛 `InvalidCursor`，pin 与消费者随恢复清空，需要按新 floor/head 重新登记；副本切换（`follower=True`）保留上游身份与游标签名密钥，已存在的有效游标可继续使用。

diagnostics.inspect(deep=True) 检查 SQLite 完整性、外键、提交连续性、当前索引与文档投影；fingerprint 比较指定读视图的键、文档版本、内容及到期事实。前者不能证明所有业务数据正确，后者相同也不能代替模式与索引定义比对。checkpoint 返回 SQLite 的 busy、log、checkpointed 信息，并不承诺截断 WAL。

examples.observations 的 contention 会让独立连接暂持 IMMEDIATE 锁，记录第二句柄的 busy 超时及释放后的成功；retention_pressure 记录读保留点和消费者分别阻止水位推进的现象。它们用独立临时文件，不接触生产路径。观察包含真实修订和异常，适合复核运行边界。

关闭所有调用方句柄再移动数据目录；不支持共享网络盘或多主写入。在空间紧张时先核实 pin/消费者，推进已完成的确认后再回收；回收不自动缩小文件，物理容量由 SQLite 空闲页复用。到期清理只在显式调用时执行，不能把墙钟到期等同于读视图变动。
