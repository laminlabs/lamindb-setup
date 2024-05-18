# Changelog

<!-- prettier-ignore -->
Name | PR | Developer | Date | Version
--- | --- | --- | --- | ---
🐛 Check empty after storage record root init in delete | [763](https://github.com/laminlabs/lamindb-setup/pull/763) | [Koncopd](https://github.com/Koncopd) | 2024-05-18 |
✨ Call `access_aws` for all paths and cache | [762](https://github.com/laminlabs/lamindb-setup/pull/762) | [Koncopd](https://github.com/Koncopd) | 2024-05-18 |
⚡️ Speed-up file hash | [761](https://github.com/laminlabs/lamindb-setup/pull/761) | [Koncopd](https://github.com/Koncopd) | 2024-05-11 | 0.71.4
♻️ Account for public storage locations | [758](https://github.com/laminlabs/lamindb-setup/pull/758) | [falexwolf](https://github.com/falexwolf) | 2024-05-10 |
♻️ Make init_instance_hub idempotent | [757](https://github.com/laminlabs/lamindb-setup/pull/757) | [falexwolf](https://github.com/falexwolf) | 2024-05-09 |
♻️ Refactor delete & connect | [756](https://github.com/laminlabs/lamindb-setup/pull/756) | [falexwolf](https://github.com/falexwolf) | 2024-05-09 |
🔥 Remove laminapp-admin logic | [755](https://github.com/laminlabs/lamindb-setup/pull/755) | [falexwolf](https://github.com/falexwolf) | 2024-05-08 |
♻️ Better treatment of current_user_id | [754](https://github.com/laminlabs/lamindb-setup/pull/754) | [falexwolf](https://github.com/falexwolf) | 2024-05-06 | 0.71.2
💚 Fix tests | [753](https://github.com/laminlabs/lamindb-setup/pull/753) | [fredericenard](https://github.com/fredericenard) | 2024-05-06 |
♻️ Actually use local db | [751](https://github.com/laminlabs/lamindb-setup/pull/751) | [falexwolf](https://github.com/falexwolf) | 2024-05-06 | 0.71.2
✏️ Restore view_tree py3.9 compatibility | [750](https://github.com/laminlabs/lamindb-setup/pull/750) | [sunnyosun](https://github.com/sunnyosun) | 2024-05-06 |
💚 Pin laminapp-ui | [749](https://github.com/laminlabs/lamindb-setup/pull/749) | [falexwolf](https://github.com/falexwolf) | 2024-05-06 |
♻️ Extend valid suffixes to composite suffixes | [746](https://github.com/laminlabs/lamindb-setup/pull/746) | [falexwolf](https://github.com/falexwolf) | 2024-05-03 | 0.71.1
🐛 Fix test failures | [745](https://github.com/laminlabs/lamindb-setup/pull/745) | [Koncopd](https://github.com/Koncopd) | 2024-05-03 |
♻️ Persist keep-artifacts-local in settings.env | [743](https://github.com/laminlabs/lamindb-setup/pull/743) | [falexwolf](https://github.com/falexwolf) | 2024-05-03 |
✅ Expand tests to multi users for managed storage | [742](https://github.com/laminlabs/lamindb-setup/pull/742) | [falexwolf](https://github.com/falexwolf) | 2024-05-03 |
🐛 Fix delete function | [741](https://github.com/laminlabs/lamindb-setup/pull/741) | [falexwolf](https://github.com/falexwolf) | 2024-04-30 | 0.71.0
✨ Manage multiple storage locations with integrity | [738](https://github.com/laminlabs/lamindb-setup/pull/738) | [falexwolf](https://github.com/falexwolf) | 2024-04-30 |
✨ Proper progress bars for upload and download | [739](https://github.com/laminlabs/lamindb-setup/pull/739) | [Koncopd](https://github.com/Koncopd) | 2024-04-28 |
♻️ Truncate fsspec deps | [740](https://github.com/laminlabs/lamindb-setup/pull/740) | [Koncopd](https://github.com/Koncopd) | 2024-04-28 |
♻️ Refactor storage management | [734](https://github.com/laminlabs/lamindb-setup/pull/734) | [falexwolf](https://github.com/falexwolf) | 2024-04-27 |
⬆️ Upper and lower bounds for fsspec | [736](https://github.com/laminlabs/lamindb-setup/pull/736) | [Koncopd](https://github.com/Koncopd) | 2024-04-27 |
♻️ Capitalize HOSTED | [733](https://github.com/laminlabs/lamindb-setup/pull/733) | [falexwolf](https://github.com/falexwolf) | 2024-04-26 |
♻️ Refactor noxfile and manage `aws` extra here instead of in lamindb | [732](https://github.com/laminlabs/lamindb-setup/pull/732) | [falexwolf](https://github.com/falexwolf) | 2024-04-25 |
🏷️ Add 3.10 types & ruff | [731](https://github.com/laminlabs/lamindb-setup/pull/731) | [falexwolf](https://github.com/falexwolf) | 2024-04-25 |
♻️ Backward compat for older botocore | [730](https://github.com/laminlabs/lamindb-setup/pull/730) | [falexwolf](https://github.com/falexwolf) | 2024-04-25 |
✨ Introduce `local_storage` mode for cloud instances | [728](https://github.com/laminlabs/lamindb-setup/pull/728) | [falexwolf](https://github.com/falexwolf) | 2024-04-23 | 0.70.0
🚚 Make `.uuid` attributes private | [727](https://github.com/laminlabs/lamindb-setup/pull/727) | [falexwolf](https://github.com/falexwolf) | 2024-04-23 |
🚚 Rename `is_cloud` to `type_is_cloud` | [726](https://github.com/laminlabs/lamindb-setup/pull/726) | [falexwolf](https://github.com/falexwolf) | 2024-04-23 |
🚸 In `.delete()`, check for data deletion & delete from hub | [725](https://github.com/laminlabs/lamindb-setup/pull/725) | [falexwolf](https://github.com/falexwolf) | 2024-04-22 |
✨ Upload dirs inplace | [722](https://github.com/laminlabs/lamindb-setup/pull/722) | [Koncopd](https://github.com/Koncopd) | 2024-04-20 | 0.69.5
♻️ Do not always read from local cache | [720](https://github.com/laminlabs/lamindb-setup/pull/720) | [falexwolf](https://github.com/falexwolf) | 2024-04-19 | 0.69.4
♻️ Improve suffix handling | [718](https://github.com/laminlabs/lamindb-setup/pull/718) | [falexwolf](https://github.com/falexwolf) | 2024-04-19 | 0.69.3
✨ Enable hybrid storage mode | [717](https://github.com/laminlabs/lamindb-setup/pull/717) | [falexwolf](https://github.com/falexwolf) | 2024-04-16 | 0.69.1
🚸 Better migrations warning | [716](https://github.com/laminlabs/lamindb-setup/pull/716) | [falexwolf](https://github.com/falexwolf) | 2024-04-16 |
🚸 Refresh token also upon access-aws | [715](https://github.com/laminlabs/lamindb-setup/pull/715) | [falexwolf](https://github.com/falexwolf) | 2024-04-15 |
🚸 Skip hub request for a purely local instance | [713](https://github.com/laminlabs/lamindb-setup/pull/713) | [falexwolf](https://github.com/falexwolf) | 2024-04-08 | 0.69.0
🐛 Fix clashes for multi process | [712](https://github.com/laminlabs/lamindb-setup/pull/712) | [falexwolf](https://github.com/falexwolf) | 2024-04-08 |
♻️ No longer need AWS account | [711](https://github.com/laminlabs/lamindb-setup/pull/711) | [falexwolf](https://github.com/falexwolf) | 2024-04-08 |
📝 Fix docs | [709](https://github.com/laminlabs/lamindb-setup/pull/709) | [falexwolf](https://github.com/falexwolf) | 2024-04-05 |
♻️ Replace TypeVar with TypeAlias | [707](https://github.com/laminlabs/lamindb-setup/pull/707) | [falexwolf](https://github.com/falexwolf) | 2024-04-04 |
♻️ Refactor clean up | [706](https://github.com/laminlabs/lamindb-setup/pull/706) | [falexwolf](https://github.com/falexwolf) | 2024-04-04 |
🩹 Do bucket injection trick only for s3 | [705](https://github.com/laminlabs/lamindb-setup/pull/705) | [Koncopd](https://github.com/Koncopd) | 2024-04-04 |
🚸 Add region param | [704](https://github.com/laminlabs/lamindb-setup/pull/704) | [falexwolf](https://github.com/falexwolf) | 2024-04-04 |
🐛 Avoid the error due to deleting the same file by multiple processes | [703](https://github.com/laminlabs/lamindb-setup/pull/703) | [Koncopd](https://github.com/Koncopd) | 2024-04-04 | 0.68.5
🐛 Fix directory uploads for hosted instances | [702](https://github.com/laminlabs/lamindb-setup/pull/702) | [Koncopd](https://github.com/Koncopd) | 2024-04-04 |
🚑️ Temp fix for non-hosted buckets | [700](https://github.com/laminlabs/lamindb-setup/pull/700) | [sunnyosun](https://github.com/sunnyosun) | 2024-04-02 |
🍱 Add the scverse spatial hackathon bucket | [699](https://github.com/laminlabs/lamindb-setup/pull/699) | [falexwolf](https://github.com/falexwolf) | 2024-03-30 |
♻️ More fine-grained db credentials management | [698](https://github.com/laminlabs/lamindb-setup/pull/698) | [falexwolf](https://github.com/falexwolf) | 2024-03-29 | 0.68.2
✨ `upath.to_url()` | [695](https://github.com/laminlabs/lamindb-setup/pull/695) | [falexwolf](https://github.com/falexwolf) | 2024-03-28 |
♻️ Enable to choose skip_suffixes | [693](https://github.com/laminlabs/lamindb-setup/pull/693) | [falexwolf](https://github.com/falexwolf) | 2024-03-27 | 0.68.1
♻️ Refactor GCP compat | [692](https://github.com/laminlabs/lamindb-setup/pull/692) | [falexwolf](https://github.com/falexwolf) | 2024-03-26 |
✨ Rework synchronize, support directories | [687](https://github.com/laminlabs/lamindb-setup/pull/687) | [Koncopd](https://github.com/Koncopd) | 2024-03-23 | 0.68.0
🎨 Refactor `delete_instance_by_id` and `delete_instance_by_slug` | [691](https://github.com/laminlabs/lamindb-setup/pull/691) | [bpenteado](https://github.com/bpenteado) | 2024-03-20 |
✨ Create utility to delete instance by id in the hub | [689](https://github.com/laminlabs/lamindb-setup/pull/689) | [bpenteado](https://github.com/bpenteado) | 2024-03-20 |
🚸 Enforce empty storage during instance deletion | [682](https://github.com/laminlabs/lamindb-setup/pull/682) | [bpenteado](https://github.com/bpenteado) | 2024-03-20 |
🐛 Use `root_as_str` for lazy load to save settings | [685](https://github.com/laminlabs/lamindb-setup/pull/685) | [Koncopd](https://github.com/Koncopd) | 2024-03-18 | 0.67.1
♻️ Enable easy switching between prod-test and staging-test | [684](https://github.com/laminlabs/lamindb-setup/pull/684) | [falexwolf](https://github.com/falexwolf) | 2024-03-17 | 0.67.0
🚚 Rename deletion functions | [683](https://github.com/laminlabs/lamindb-setup/pull/683) | [bpenteado](https://github.com/bpenteado) | 2024-03-15 |
✨ Load `git_repo` setting from hub, refactor settings file management | [681](https://github.com/laminlabs/lamindb-setup/pull/681) | [falexwolf](https://github.com/falexwolf) | 2024-03-14 |
✨ Create gating function to check for empty storage location | [680](https://github.com/laminlabs/lamindb-setup/pull/680) | [bpenteado](https://github.com/bpenteado) | 2024-03-14 |
👷 Use test utilities from `laminhub-rest` during local tests | [679](https://github.com/laminlabs/lamindb-setup/pull/679) | [bpenteado](https://github.com/bpenteado) | 2024-03-13 |
♻️ Refactor view_tree() | [678](https://github.com/laminlabs/lamindb-setup/pull/678) | [falexwolf](https://github.com/falexwolf) | 2024-03-12 |
✨ Add hash_code function | [677](https://github.com/laminlabs/lamindb-setup/pull/677) | [falexwolf](https://github.com/falexwolf) | 2024-03-12 |
🎨 Make `get_stat_dir_s3` compatible with canonical storage policy (`s3:ListBucket` conditional on `prefix/`) | [675](https://github.com/laminlabs/lamindb-setup/pull/675) | [bpenteado](https://github.com/bpenteado) | 2024-03-11 |
🚚 Rename `lnschema_lamin1` to `wetlab` | [676](https://github.com/laminlabs/lamindb-setup/pull/676) | [falexwolf](https://github.com/falexwolf) | 2024-03-09 |
🚸 Restore `auto_connect=True` after `lamin init` | [674](https://github.com/laminlabs/lamindb-setup/pull/674) | [falexwolf](https://github.com/falexwolf) | 2024-03-08 | 0.66.1
🐛 Fix hosted regions list | [673](https://github.com/laminlabs/lamindb-setup/pull/673) | [Koncopd](https://github.com/Koncopd) | 2024-03-07 | 0.66.0
🚑️ Fix bionty reload | [672](https://github.com/laminlabs/lamindb-setup/pull/672) | [sunnyosun](https://github.com/sunnyosun) | 2024-03-07 |
🩹 Nicer behavior for same instance | [671](https://github.com/laminlabs/lamindb-setup/pull/671) | [falexwolf](https://github.com/falexwolf) | 2024-03-07 |
🏗️ Enable`ln.connect()` in lamindb | [668](https://github.com/laminlabs/lamindb-setup/pull/668) | [falexwolf](https://github.com/falexwolf) | 2024-03-07 |
🚚 Rename `load()` to `connect()` | [667](https://github.com/laminlabs/lamindb-setup/pull/667) | [falexwolf](https://github.com/falexwolf) | 2024-03-07 |
👷 Better CI group names | [666](https://github.com/laminlabs/lamindb-setup/pull/666) | [falexwolf](https://github.com/falexwolf) | 2024-03-07 |
🛂 Make `get_stat_dir_s3` read from UPath credentials | [664](https://github.com/laminlabs/lamindb-setup/pull/664) | [bpenteado](https://github.com/bpenteado) | 2024-03-06 |
♻️ Move generic hashing and storage utilities from `lamindb` into `lamindb-setup` | [661](https://github.com/laminlabs/lamindb-setup/pull/661) | [bpenteado](https://github.com/bpenteado) | 2024-03-06 |
♻️ Make `SetupSettings` a dynamic class | [663](https://github.com/laminlabs/lamindb-setup/pull/663) | [falexwolf](https://github.com/falexwolf) | 2024-03-06 |
♻️ Replace `InstanceSettings.identifier` with `InstanceSettings.slug` | [662](https://github.com/laminlabs/lamindb-setup/pull/662) | [falexwolf](https://github.com/falexwolf) | 2024-03-06 |
♻️ Simplify path types & point to hub mono-repo | [660](https://github.com/laminlabs/lamindb-setup/pull/660) | [falexwolf](https://github.com/falexwolf) | 2024-03-05 |
🚚 Rename `.dev` to `.core` | [659](https://github.com/laminlabs/lamindb-setup/pull/659) | [falexwolf](https://github.com/falexwolf) | 2024-03-05 |
♻️ Add `UPathStr` type | [658](https://github.com/laminlabs/lamindb-setup/pull/658) | [falexwolf](https://github.com/falexwolf) | 2024-03-05 |
🩹 Resolve cache_dir in the setter | [657](https://github.com/laminlabs/lamindb-setup/pull/657) | [Koncopd](https://github.com/Koncopd) | 2024-03-04 |
♻️ Pass `storage_root` to `access_aws` | [656](https://github.com/laminlabs/lamindb-setup/pull/656) | [falexwolf](https://github.com/falexwolf) | 2024-02-29 | 0.65.3
♻️ Allow passing `access_token` directly | [655](https://github.com/laminlabs/lamindb-setup/pull/655) | [Koncopd](https://github.com/Koncopd) | 2024-02-29 |
✨ Add create_mapper util | [654](https://github.com/laminlabs/lamindb-setup/pull/654) | [Koncopd](https://github.com/Koncopd) | 2024-02-19 |
🐛 Fix aws_access calls in CLI | [652](https://github.com/laminlabs/lamindb-setup/pull/652) | [Koncopd](https://github.com/Koncopd) | 2024-02-18 |
✏️ pre-commit on all files | [650](https://github.com/laminlabs/lamindb-setup/pull/650) | [Koncopd](https://github.com/Koncopd) | 2024-02-17 |
🐛 Use shutil.move to move the sqlite file when changing cache folder | [649](https://github.com/laminlabs/lamindb-setup/pull/649) | [Koncopd](https://github.com/Koncopd) | 2024-02-17 |
:speaker: Better error message for differing db dsn | [648](https://github.com/laminlabs/lamindb-setup/pull/648) | [falexwolf](https://github.com/falexwolf) | 2024-02-13 |
♻️ Add ability to choose region | [647](https://github.com/laminlabs/lamindb-setup/pull/647) | [falexwolf](https://github.com/falexwolf) | 2024-02-07 |
💄 Prettify hosted storage notebooks | [646](https://github.com/laminlabs/lamindb-setup/pull/646) | [falexwolf](https://github.com/falexwolf) | 2024-02-05 |
📝 Fix error raising for old deleting env file | [645](https://github.com/laminlabs/lamindb-setup/pull/645) | [sunnyosun](https://github.com/sunnyosun) | 2024-02-05 |
♻️ Check manual credentials in path.fs, actually reuse anon | [644](https://github.com/laminlabs/lamindb-setup/pull/644) | [Koncopd](https://github.com/Koncopd) | 2024-02-05 |
⚡️ Speed up lamindb import & loading time | [643](https://github.com/laminlabs/lamindb-setup/pull/643) | [falexwolf](https://github.com/falexwolf) | 2024-02-03 |
♻️ Iterate on importdb and exportdb | [642](https://github.com/laminlabs/lamindb-setup/pull/642) | [falexwolf](https://github.com/falexwolf) | 2024-02-03 |
♻️ Add backward compat for delete_instance call | [641](https://github.com/laminlabs/lamindb-setup/pull/641) | [falexwolf](https://github.com/falexwolf) | 2024-02-03 |
♻️ Refactor loading instances and add test for loading hosted instance | [640](https://github.com/laminlabs/lamindb-setup/pull/640) | [falexwolf](https://github.com/falexwolf) | 2024-02-02 | 0.65.1
♻️ Low-latency storage permission management | [639](https://github.com/laminlabs/lamindb-setup/pull/639) | [falexwolf](https://github.com/falexwolf) | 2024-02-02 |
✅ Implement proper integration tests for hosted storage | [638](https://github.com/laminlabs/lamindb-setup/pull/638) | [falexwolf](https://github.com/falexwolf) | 2024-02-02 |
🐛 Fix permission error for init of hosted instances | [636](https://github.com/laminlabs/lamindb-setup/pull/636) | [Koncopd](https://github.com/Koncopd) | 2024-02-02 |
📝 Add examples to `Upath.rename` | [637](https://github.com/laminlabs/lamindb-setup/pull/637) | [sunnyosun](https://github.com/sunnyosun) | 2024-02-02 |
♻️ Add using key | [633](https://github.com/laminlabs/lamindb-setup/pull/633) | [falexwolf](https://github.com/falexwolf) | 2024-01-30 | 0.65.0
✨ Enable multiple instances | [632](https://github.com/laminlabs/lamindb-setup/pull/632) | [falexwolf](https://github.com/falexwolf) | 2024-01-30 |
🐛 Fix import bionty-base | [631](https://github.com/laminlabs/lamindb-setup/pull/631) | [sunnyosun](https://github.com/sunnyosun) | 2024-01-30 |
💚 Fixes for lamindb | [630](https://github.com/laminlabs/lamindb-setup/pull/630) | [falexwolf](https://github.com/falexwolf) | 2024-01-29 |
🎨 Refactor and define hosted storage user journey | [626](https://github.com/laminlabs/lamindb-setup/pull/626) | [falexwolf](https://github.com/falexwolf) | 2024-01-29 |
💚 Fix register calls | [628](https://github.com/laminlabs/lamindb-setup/pull/628) | [falexwolf](https://github.com/falexwolf) | 2024-01-29 |
🔥 Remove vault | [625](https://github.com/laminlabs/lamindb-setup/pull/625) | [falexwolf](https://github.com/falexwolf) | 2024-01-26 |
✨ Introduce create storage | [624](https://github.com/laminlabs/lamindb-setup/pull/624) | [falexwolf](https://github.com/falexwolf) | 2024-01-26 |
✨ Add storage client | [621](https://github.com/laminlabs/lamindb-setup/pull/621) | [falexwolf](https://github.com/falexwolf) | 2024-01-26 |
💚 Fix tests | [623](https://github.com/laminlabs/lamindb-setup/pull/623) | [falexwolf](https://github.com/falexwolf) | 2024-01-26 |
🎨 Move default storages to folders | [622](https://github.com/laminlabs/lamindb-setup/pull/622) | [falexwolf](https://github.com/falexwolf) | 2024-01-26 |
🎨 Remove call to `create_path` inside `path.view_tree` | [618](https://github.com/laminlabs/lamindb-setup/pull/618) | [mukund109](https://github.com/mukund109) | 2024-01-26 |
🐛 Fix option inheritance in create_path and add tests | [617](https://github.com/laminlabs/lamindb-setup/pull/617) | [Koncopd](https://github.com/Koncopd) | 2024-01-12 |
✅ Update tests relying on laminhub-rest | [616](https://github.com/laminlabs/lamindb-setup/pull/616) | [fredericenard](https://github.com/fredericenard) | 2024-01-12 |
🚚 Rename .bionty to .public | [615](https://github.com/laminlabs/lamindb-setup/pull/615) | [sunnyosun](https://github.com/sunnyosun) | 2024-01-10 | 0.64.2
🚸 Fix connection timeout | [611](https://github.com/laminlabs/lamindb-setup/pull/611) | [falexwolf](https://github.com/falexwolf) | 2024-01-08 | 0.64.1
🚸 Add anonymous access (now works without login) | [610](https://github.com/laminlabs/lamindb-setup/pull/610) | [falexwolf](https://github.com/falexwolf) | 2024-01-06 | 0.64.0
💚 Fix tests in local hub | [609](https://github.com/laminlabs/lamindb-setup/pull/609) | [falexwolf](https://github.com/falexwolf) | 2024-01-05 |
🚚 Migrate foreign key constraint for new hub | [608](https://github.com/laminlabs/lamindb-setup/pull/608) | [falexwolf](https://github.com/falexwolf) | 2024-01-05 |
💚 Upgrade to Python 3.10 | [607](https://github.com/laminlabs/lamindb-setup/pull/607) | [falexwolf](https://github.com/falexwolf) | 2024-01-05 |
🚚 Prepare a potential migration of the hub to Django | [606](https://github.com/laminlabs/lamindb-setup/pull/606) | [falexwolf](https://github.com/falexwolf) | 2024-01-05 | 0.63.1
♻️ Refactor local sign up | [605](https://github.com/laminlabs/lamindb-setup/pull/605) | [falexwolf](https://github.com/falexwolf) | 2024-01-02 |
🎨 Write lamindb version to hub | [603](https://github.com/laminlabs/lamindb-setup/pull/603) | [falexwolf](https://github.com/falexwolf) | 2024-01-01 | 0.63.0
♻️ Do no longer validate uniqueness sqlite | [604](https://github.com/laminlabs/lamindb-setup/pull/604) | [falexwolf](https://github.com/falexwolf) | 2023-12-31 |
🐛 Fix bionty settings | [602](https://github.com/laminlabs/lamindb-setup/pull/602) | [falexwolf](https://github.com/falexwolf) | 2023-12-22 |
♻️ Adopt SQLite locker to id naming and move `exclusion/` inside `.lamindb/` | [601](https://github.com/laminlabs/lamindb-setup/pull/601) | [falexwolf](https://github.com/falexwolf) | 2023-12-21 | 0.62.0
🎨 Name `.lndb` files by instance id | [600](https://github.com/laminlabs/lamindb-setup/pull/600) | [falexwolf](https://github.com/falexwolf) | 2023-12-21 |
🐛 Do not unlock cloud sqlite instance on migrate deploy | [599](https://github.com/laminlabs/lamindb-setup/pull/599) | [Koncopd](https://github.com/Koncopd) | 2023-12-20 |
🔊 Warn on sync of non-existing paths | [598](https://github.com/laminlabs/lamindb-setup/pull/598) | [Koncopd](https://github.com/Koncopd) | 2023-12-20 |
🚸 Silence httpx logger | [597](https://github.com/laminlabs/lamindb-setup/pull/597) | [falexwolf](https://github.com/falexwolf) | 2023-12-13 | 0.61.4
🚸 Detect new schema modules | [596](https://github.com/laminlabs/lamindb-setup/pull/596) | [falexwolf](https://github.com/falexwolf) | 2023-12-13 | 0.61.3
⬆️ Bump supabase to latest version | [594](https://github.com/laminlabs/lamindb-setup/pull/594) | [falexwolf](https://github.com/falexwolf) | 2023-12-11 | 0.61.2
♻️ Defer storage API access to actual access | [593](https://github.com/laminlabs/lamindb-setup/pull/593) | [falexwolf](https://github.com/falexwolf) | 2023-12-07 | 0.61.1
🚸 Enable schema packages that do not start with lnschema_ | [592](https://github.com/laminlabs/lamindb-setup/pull/592) | [falexwolf](https://github.com/falexwolf) | 2023-11-28 | 0.61.0
🚑️ Proper fix for trailing slash in create_path | [591](https://github.com/laminlabs/lamindb-setup/pull/591) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-28 |
🚑️ Fix trailing slash for UPath view_tree | [590](https://github.com/laminlabs/lamindb-setup/pull/590) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-28 |
🔊 Do not print progress by default | [588](https://github.com/laminlabs/lamindb-setup/pull/588) | [Koncopd](https://github.com/Koncopd) | 2023-11-25 |
✨ Set cache via LAMIN_CACHE_DIR env variable | [589](https://github.com/laminlabs/lamindb-setup/pull/589) | [Koncopd](https://github.com/Koncopd) | 2023-11-25 |
🚚 Centralize progressbar and convert to percentage | [587](https://github.com/laminlabs/lamindb-setup/pull/587) | [falexwolf](https://github.com/falexwolf) | 2023-11-24 | 0.60.0
🔊 Log an error when assertion is false | [586](https://github.com/laminlabs/lamindb-setup/pull/586) | [fredericenard](https://github.com/fredericenard) | 2023-11-22 |
🔊 Polish logging in view_tree() | [584](https://github.com/laminlabs/lamindb-setup/pull/584) | [falexwolf](https://github.com/falexwolf) | 2023-11-15 | 0.59.1
🔇 Do not log migrations during init | [583](https://github.com/laminlabs/lamindb-setup/pull/583) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-14 |
♻️ Prettify view_tree | [582](https://github.com/laminlabs/lamindb-setup/pull/582) | [falexwolf](https://github.com/falexwolf) | 2023-11-14 | 0.59.0
🔧 Detect both missing and ahead migrations | [581](https://github.com/laminlabs/lamindb-setup/pull/581) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-14 |
✨ Added check command for dev | [580](https://github.com/laminlabs/lamindb-setup/pull/580) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-13 |
⏪️ Remove reset sources for bionty | [579](https://github.com/laminlabs/lamindb-setup/pull/579) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-10 |
🚚 Move `view_tree` from lamindb here | [578](https://github.com/laminlabs/lamindb-setup/pull/578) | [falexwolf](https://github.com/falexwolf) | 2023-11-09 | 0.58.0
♻️  Refactor `UPath` | [577](https://github.com/laminlabs/lamindb-setup/pull/577) | [falexwolf](https://github.com/falexwolf) | 2023-11-09 |
🎨 Set bionty versionsdir according to settings_dir | [576](https://github.com/laminlabs/lamindb-setup/pull/576) | [sunnyosun](https://github.com/sunnyosun) | 2023-11-09 |
🎨 Raise exceptions | [575](https://github.com/laminlabs/lamindb-setup/pull/575) | [falexwolf](https://github.com/falexwolf) | 2023-11-09 | 0.57.3
✨ Enable using SETTINGS_DIR env variable to use AWS Lambda | [574](https://github.com/laminlabs/lamindb-setup/pull/574) | [fredericenard](https://github.com/fredericenard) | 2023-11-05 | 0.57.2
🚸 Lower `conn_mag_age` | [573](https://github.com/laminlabs/lamindb-setup/pull/573) | [falexwolf](https://github.com/falexwolf) | 2023-11-04 | 0.57.1
🚚 Move CLI to lamindb | [572](https://github.com/laminlabs/lamindb-setup/pull/572) | [falexwolf](https://github.com/falexwolf) | 2023-10-30 | 0.57.0
♻️ Also enable to read db from env variable | [571](https://github.com/laminlabs/lamindb-setup/pull/571) | [falexwolf](https://github.com/falexwolf) | 2023-10-26 | 0.56.4
♻️ Enable to pre-define the instance id | [570](https://github.com/laminlabs/lamindb-setup/pull/570) | [falexwolf](https://github.com/falexwolf) | 2023-10-26 | 0.56.3
🐛 Fix read-only access | [569](https://github.com/laminlabs/lamindb-setup/pull/569) | [falexwolf](https://github.com/falexwolf) | 2023-10-26 | 0.56.2
✨ Set notebook files to be hidden | [568](https://github.com/laminlabs/lamindb-setup/pull/568) | [sunnyosun](https://github.com/sunnyosun) | 2023-10-26 | 0.56.1
✨ Read db URL from local parameters | [567](https://github.com/laminlabs/lamindb-setup/pull/567) | [falexwolf](https://github.com/falexwolf) | 2023-10-26 | 0.56.0
♻️ Do no longer store the db connection string in hub | [566](https://github.com/laminlabs/lamindb-setup/pull/566) | [falexwolf](https://github.com/falexwolf) | 2023-10-25 |
👷 Run vault tests in staging environment | [565](https://github.com/laminlabs/lamindb-setup/pull/565) | [fredericenard](https://github.com/fredericenard) | 2023-10-25 |
📌 Unpin vault and move to dev dependencies | [562](https://github.com/laminlabs/lamindb-setup/pull/562) | [fredericenard](https://github.com/fredericenard) | 2023-10-20 | 0.55.6
🔇 Remove hint about registering instance | [564](https://github.com/laminlabs/lamindb-setup/pull/564) | [falexwolf](https://github.com/falexwolf) | 2023-10-20 |
✨ Add django interface | [563](https://github.com/laminlabs/lamindb-setup/pull/563) | [falexwolf](https://github.com/falexwolf) | 2023-10-20 |
🔥 Do not populate user email in User registry | [561](https://github.com/laminlabs/lamindb-setup/pull/561) | [falexwolf](https://github.com/falexwolf) | 2023-10-19 | 0.55
♻️ Better synching of email address from hub to instance | [560](https://github.com/laminlabs/lamindb-setup/pull/560) | [falexwolf](https://github.com/falexwolf) | 2023-10-19 |
🚚 Rename `Species` to `Organism` | [559](https://github.com/laminlabs/lamindb-setup/pull/559) | [sunnyosun](https://github.com/sunnyosun) | 2023-10-19 | 0.55.4
🔥 Remove deploy-server command | [558](https://github.com/laminlabs/lamindb-setup/pull/558) | [falexwolf](https://github.com/falexwolf) | 2023-10-19 | 0.55.2
♻️ Refactor and introduce API key | [557](https://github.com/laminlabs/lamindb-setup/pull/557) | [falexwolf](https://github.com/falexwolf) | 2023-10-19 |
✨ Cache management | [554](https://github.com/laminlabs/lamindb-setup/pull/554) | [Koncopd](https://github.com/Koncopd) | 2023-10-16 | 0.55.1
🐛 Fix load after failed load in test-load-lock.ipynb | [556](https://github.com/laminlabs/lamindb-setup/pull/556) | [Koncopd](https://github.com/Koncopd) | 2023-10-15 |
💚 Fix CI | [555](https://github.com/laminlabs/lamindb-setup/pull/555) | [falexwolf](https://github.com/falexwolf) | 2023-10-13 |
🚚 Rename `UserSettings.id` to `UserSettings.uid` | [552](https://github.com/laminlabs/lamindb-setup/pull/552) | [falexwolf](https://github.com/falexwolf) | 2023-10-13 | 0.55.0
🔇 Silence logger in main | [553](https://github.com/laminlabs/lamindb-setup/pull/553) | [sunnyosun](https://github.com/sunnyosun) | 2023-10-10 |
💄 Prettify logging | [550](https://github.com/laminlabs/lamindb-setup/pull/550) | [falexwolf](https://github.com/falexwolf) | 2023-10-09 |
🚸 Silence loggers in CLI | [551](https://github.com/laminlabs/lamindb-setup/pull/551) | [falexwolf](https://github.com/falexwolf) | 2023-10-09 |
🔧 Upper bound Django to 4.3 | [549](https://github.com/laminlabs/lamindb-setup/pull/549) | [falexwolf](https://github.com/falexwolf) | 2023-10-09 |
🔒 Enable to generate public read db connection string | [548](https://github.com/laminlabs/lamindb-setup/pull/548) | [fredericenard](https://github.com/fredericenard) | 2023-10-05 |
🚸 Allow saving notebooks of other users | [547](https://github.com/laminlabs/lamindb-setup/pull/547) | [falexwolf](https://github.com/falexwolf) | 2023-10-04 | 0.54.3
🔊 Add more logging | [546](https://github.com/laminlabs/lamindb-setup/pull/546) | [falexwolf](https://github.com/falexwolf) | 2023-10-04 |
🔒 Enforce compatibility with lamin-vault 0.0.7 | [545](https://github.com/laminlabs/lamindb-setup/pull/545) | [fredericenard](https://github.com/fredericenard) | 2023-10-03 |
🐛 Prevent using vault by default when loading an instance | [544](https://github.com/laminlabs/lamindb-setup/pull/544) | [fredericenard](https://github.com/fredericenard) | 2023-10-03 | 0.54.2
🚸 Do not use vault for SQLite instances | [543](https://github.com/laminlabs/lamindb-setup/pull/543) | [falexwolf](https://github.com/falexwolf) | 2023-10-02 | 0.54.2
✅ Complete notebook tests | [542](https://github.com/laminlabs/lamindb-setup/pull/542) | [falexwolf](https://github.com/falexwolf) | 2023-10-02 | 0.54.1
🐛 Fix vault import | [541](https://github.com/laminlabs/lamindb-setup/pull/541) | [fredericenard](https://github.com/fredericenard) | 2023-10-01 | 0.54.0
♻️ Move dependencies to dev | [540](https://github.com/laminlabs/lamindb-setup/pull/540) | [falexwolf](https://github.com/falexwolf) | 2023-10-01 |
✨ View schema interactively | [539](https://github.com/laminlabs/lamindb-setup/pull/539) | [falexwolf](https://github.com/falexwolf) | 2023-10-01 |
✨ Add notebook save functionality | [537](https://github.com/laminlabs/lamindb-setup/pull/537) | [falexwolf](https://github.com/falexwolf) | 2023-10-01 |
✨ Enabling server deployment from CLI | [536](https://github.com/laminlabs/lamindb-setup/pull/536) | [fredericenard](https://github.com/fredericenard) | 2023-09-30 |
✨ Manage db connection with vault | [533](https://github.com/laminlabs/lamindb-setup/pull/533) | [fredericenard](https://github.com/fredericenard) | 2023-09-29 |
♻️ Refactor optional cloud sqlite locking and unlocking | [535](https://github.com/laminlabs/lamindb-setup/pull/535) | [Koncopd](https://github.com/Koncopd) | 2023-09-23 |
✨ Add squashmigrations | [534](https://github.com/laminlabs/lamindb-setup/pull/534) | [sunnyosun](https://github.com/sunnyosun) | 2023-09-22 |
♻️ Do not lock instance on load for laminapp-admin | [532](https://github.com/laminlabs/lamindb-setup/pull/532) | [falexwolf](https://github.com/falexwolf) | 2023-09-21 | 0.53.2
♻️ Refactor lock | [530](https://github.com/laminlabs/lamindb-setup/pull/530) | [falexwolf](https://github.com/falexwolf) | 2023-09-20 |
💚 Try to fix noaws test | [528](https://github.com/laminlabs/lamindb-setup/pull/528) | [falexwolf](https://github.com/falexwolf) | 2023-09-20 |
📝 Fix loading message of lamindb | [526](https://github.com/laminlabs/lamindb-setup/pull/526) | [falexwolf](https://github.com/falexwolf) | 2023-09-17 | 0.53.1
🔐 Change lock expiration time to 1 day | [525](https://github.com/laminlabs/lamindb-setup/pull/525) | [Koncopd](https://github.com/Koncopd) | 2023-09-17 |
🐛 Unlock load on error | [523](https://github.com/laminlabs/lamindb-setup/pull/523) | [Koncopd](https://github.com/Koncopd) | 2023-09-17 |
🚚 Migrate to new hub prod URL | [524](https://github.com/laminlabs/lamindb-setup/pull/524) | [falexwolf](https://github.com/falexwolf) | 2023-09-17 |
👷 Dispatch tests | [522](https://github.com/laminlabs/lamindb-setup/pull/522) | [falexwolf](https://github.com/falexwolf) | 2023-09-15 | 0.53.0
🎨 Streamline login | [521](https://github.com/laminlabs/lamindb-setup/pull/521) | [falexwolf](https://github.com/falexwolf) | 2023-09-14 |
🎨 Use a fallback for the hub API in case we move the endpoint | [519](https://github.com/laminlabs/lamindb-setup/pull/519) | [falexwolf](https://github.com/falexwolf) | 2023-09-13 |
✨ Enable to prefix settings in any env | [520](https://github.com/laminlabs/lamindb-setup/pull/520) | [fredericenard](https://github.com/fredericenard) | 2023-09-12 |
⚡️ Speed up query for instance retrieval | [518](https://github.com/laminlabs/lamindb-setup/pull/518) | [falexwolf](https://github.com/falexwolf) | 2023-09-11 |
🎨 Do not add laminapp-admin to the instance user table | [517](https://github.com/laminlabs/lamindb-setup/pull/517) | [falexwolf](https://github.com/falexwolf) | 2023-09-11 |
♻️ Simplify & speed up load instance | [516](https://github.com/laminlabs/lamindb-setup/pull/516) | [falexwolf](https://github.com/falexwolf) | 2023-09-11 |
🔊 Show more info about the locking user | [515](https://github.com/laminlabs/lamindb-setup/pull/515) | [Koncopd](https://github.com/Koncopd) | 2023-09-09 |
♻️ Make two dev modules public, clean up workflow, noxfile, test session names | [514](https://github.com/laminlabs/lamindb-setup/pull/514) | [falexwolf](https://github.com/falexwolf) | 2023-09-08 | 0.52.0
♻️ Do not hard-code laminhub-rest path | [513](https://github.com/laminlabs/lamindb-setup/pull/513) | [falexwolf](https://github.com/falexwolf) | 2023-09-08 |
♻️ Move supabase docker from nox to conftest | [512](https://github.com/laminlabs/lamindb-setup/pull/512) | [falexwolf](https://github.com/falexwolf) | 2023-09-08 |
♻️ Refactor hub tests | [511](https://github.com/laminlabs/lamindb-setup/pull/511) | [falexwolf](https://github.com/falexwolf) | 2023-09-08 |
♻️ Streamline hub interactions, speed up hub testing | [510](https://github.com/laminlabs/lamindb-setup/pull/510) | [falexwolf](https://github.com/falexwolf) | 2023-09-07 |
✅ Add test for loading instance after private public switch | [509](https://github.com/laminlabs/lamindb-setup/pull/509) | [falexwolf](https://github.com/falexwolf) | 2023-09-07 |
♻️ Refactor hub client | [508](https://github.com/laminlabs/lamindb-setup/pull/508) | [falexwolf](https://github.com/falexwolf) | 2023-09-07 |
♻️ Cache user UUID | [507](https://github.com/laminlabs/lamindb-setup/pull/507) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
♻️ Type instance id as UUID | [506](https://github.com/laminlabs/lamindb-setup/pull/506) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
✅ Test implicit lamindb load | [503](https://github.com/laminlabs/lamindb-setup/pull/503) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
🚚 Rename lnhub-rest to laminhub-rest | [504](https://github.com/laminlabs/lamindb-setup/pull/504) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
✅ More tests for signup | [502](https://github.com/laminlabs/lamindb-setup/pull/502) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
✅ Test logger | [501](https://github.com/laminlabs/lamindb-setup/pull/501) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
👔 Always let permission query from hub override local cache for remote instances | [497](https://github.com/laminlabs/lamindb-setup/pull/497) | [bpenteado](https://github.com/bpenteado) | 2023-09-06 |
♻️ Re-organize unit tests, hub client & fix signup logging | [500](https://github.com/laminlabs/lamindb-setup/pull/500) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
👷 Re-organize tests | [499](https://github.com/laminlabs/lamindb-setup/pull/499) | [falexwolf](https://github.com/falexwolf) | 2023-09-06 |
🚸 Add env prefix to BaseSettings | [496](https://github.com/laminlabs/lamindb-setup/pull/496) | [falexwolf](https://github.com/falexwolf) | 2023-09-05 |
🔧 Narrow `django` & `dj_database_url` versions | [495](https://github.com/laminlabs/lamindb-setup/pull/495) | [falexwolf](https://github.com/falexwolf) | 2023-09-05 |
🔥 Remove legacy settings dir port code | [494](https://github.com/laminlabs/lamindb-setup/pull/494) | [falexwolf](https://github.com/falexwolf) | 2023-09-05 |
🚸 Cache instance id during init already | [493](https://github.com/laminlabs/lamindb-setup/pull/493) | [falexwolf](https://github.com/falexwolf) | 2023-08-31 | 0.51.3
🚸 Cache instance id during `load` | [492](https://github.com/laminlabs/lamindb-setup/pull/492) | [bpenteado](https://github.com/bpenteado) | 2023-08-30 | 0.51.2
♻️ Refactor to use UPath everywhere  | [490](https://github.com/laminlabs/lamindb-setup/pull/490) | [Koncopd](https://github.com/Koncopd) | 2023-08-29 | 0.51.1
🔥 Remove deprecated locker code | [491](https://github.com/laminlabs/lamindb-setup/pull/491) | [Koncopd](https://github.com/Koncopd) | 2023-08-28 |
🚸 Implement delimiter validation during `init` and `delete` | [489](https://github.com/laminlabs/lamindb-setup/pull/489) | [bpenteado](https://github.com/bpenteado) | 2023-08-25 |
♻️ Refactor notebook update | [488](https://github.com/laminlabs/lamindb-setup/pull/488) | [falexwolf](https://github.com/falexwolf) | 2023-08-23 | 0.51.0
♻️ Introduce `create_path` | [486](https://github.com/laminlabs/lamindb-setup/pull/486) | [falexwolf](https://github.com/falexwolf) | 2023-08-22 |
🚚 Rename `_storage.py` to `_settings_storage.py` | [485](https://github.com/laminlabs/lamindb-setup/pull/485) | [falexwolf](https://github.com/falexwolf) | 2023-08-22 |
✨ A function to convert pathlike to Path or UPath inheriting options from root | [484](https://github.com/laminlabs/lamindb-setup/pull/484) | [Koncopd](https://github.com/Koncopd) | 2023-08-22 |
🔊 Add more logging to closing SQLite | [483](https://github.com/laminlabs/lamindb-setup/pull/483) | [falexwolf](https://github.com/falexwolf) | 2023-08-21 |
🐛 Fix UPath check on Storage init | [482](https://github.com/laminlabs/lamindb-setup/pull/482) | [Koncopd](https://github.com/Koncopd) | 2023-08-21 |
🔥 Remove unused environment variable and fix sign up logging | [481](https://github.com/laminlabs/lamindb-setup/pull/481) | [falexwolf](https://github.com/falexwolf) | 2023-08-21 |
👷 Fix coverage compute | [480](https://github.com/laminlabs/lamindb-setup/pull/480) | [falexwolf](https://github.com/falexwolf) | 2023-08-19 |
👷 Run hub tests from `lamindb-setup` | [479](https://github.com/laminlabs/lamindb-setup/pull/479) | [bpenteado](https://github.com/bpenteado) | 2023-08-19 |
👷 Renable linting | [478](https://github.com/laminlabs/lamindb-setup/pull/478) | [falexwolf](https://github.com/falexwolf) | 2023-08-17 |
🩹 Create record in user table upon login for existing instance | [477](https://github.com/laminlabs/lamindb-setup/pull/477) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 | 0.50.2
👷 Try fixing coverage compute | [476](https://github.com/laminlabs/lamindb-setup/pull/476) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 |
🚑️ Fix loading postgres instances | [475](https://github.com/laminlabs/lamindb-setup/pull/475) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 | 0.50.1
🐛 Fix init in an empty s3 bucket | [473](https://github.com/laminlabs/lamindb-setup/pull/473) | [Koncopd](https://github.com/Koncopd) | 2023-08-16 | 0.50.0
🔥 Remove support for migrating legacy SA instances | [472](https://github.com/laminlabs/lamindb-setup/pull/472) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 |
🔥 Remove sqlalchemy as a dependency | [471](https://github.com/laminlabs/lamindb-setup/pull/471) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 |
🔥 Remove lnschema-core submodule | [470](https://github.com/laminlabs/lamindb-setup/pull/470) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 |
♻️ Refactor delete dialogue | [469](https://github.com/laminlabs/lamindb-setup/pull/469) | [falexwolf](https://github.com/falexwolf) | 2023-08-16 |
🚸 Add delete confirmation dialogue | [467](https://github.com/laminlabs/lamindb-setup/pull/467) | [Zethson](https://github.com/Zethson) | 2023-08-15 |
📝 Move `setup-user` guide to lamindb | [463](https://github.com/laminlabs/lamindb-setup/pull/463) | [falexwolf](https://github.com/falexwolf) | 2023-08-11 |
🚸 Reformat missing-migrations warning | [461](https://github.com/laminlabs/lamindb-setup/pull/461) | [falexwolf](https://github.com/falexwolf) | 2023-08-10 | 0.49.7
🐛 Fix and test close for read only buckets | [462](https://github.com/laminlabs/lamindb-setup/pull/462) | [Koncopd](https://github.com/Koncopd) | 2023-08-10 |
👷 Test configuration with no AWS access | [460](https://github.com/laminlabs/lamindb-setup/pull/460) | [falexwolf](https://github.com/falexwolf) | 2023-08-07 | 0.49.6
👷 Simplify CI setup | [458](https://github.com/laminlabs/lamindb-setup/pull/458) | [falexwolf](https://github.com/falexwolf) | 2023-08-07 |
🎨 Use logger.save | [459](https://github.com/laminlabs/lamindb-setup/pull/459) | [sunnyosun](https://github.com/sunnyosun) | 2023-08-07 |
🐛 Fix aws access with no credentials | [457](https://github.com/laminlabs/lamindb-setup/pull/457) | [Koncopd](https://github.com/Koncopd) | 2023-08-07 |
🔊 Refactored logging msg | [456](https://github.com/laminlabs/lamindb-setup/pull/456) | [sunnyosun](https://github.com/sunnyosun) | 2023-08-07 |
♻️ Replace `lamin_logger` with `lamin_utils` | [454](https://github.com/laminlabs/lamindb-setup/pull/454) | [falexwolf](https://github.com/falexwolf) | 2023-08-06 | 0.49.5
🚚 Rename `ORM` to `Registry` | [453](https://github.com/laminlabs/lamindb-setup/pull/453) | [sunnyosun](https://github.com/sunnyosun) | 2023-08-05 |
⬇️ Upper bound external dependencies | [452](https://github.com/laminlabs/lamindb-setup/pull/452) | [falexwolf](https://github.com/falexwolf) | 2023-08-04 | 0.49.4
🎨 Add `.record` property to `StorageSettings` | [451](https://github.com/laminlabs/lamindb-setup/pull/451) | [falexwolf](https://github.com/falexwolf) | 2023-08-03 | 0.49.3
✅ Fix init-instance test for sqlite instance | [450](https://github.com/laminlabs/lamindb-setup/pull/450) | [bpenteado](https://github.com/bpenteado) | 2023-08-02 |
🎨 Fix `init-instance` anti-patterns and expand tests | [449](https://github.com/laminlabs/lamindb-setup/pull/449) | [bpenteado](https://github.com/bpenteado) | 2023-08-01 |
✨ Propagate kwargs through synchronization functions | [448](https://github.com/laminlabs/lamindb-setup/pull/448) | [Koncopd](https://github.com/Koncopd) | 2023-07-31 | 0.49.2
🐛 Fix local variable 'db_dsn' referenced before assignment | [447](https://github.com/laminlabs/lamindb-setup/pull/447) | [fredericenard](https://github.com/fredericenard) | 2023-07-30 |
🚸 Show help by default and add --version | [446](https://github.com/laminlabs/lamindb-setup/pull/446) | [Zethson](https://github.com/Zethson) | 2023-07-30 |
🐛 Fix flawed DBUser logic in `init_instance` | [442](https://github.com/laminlabs/lamindb-setup/pull/442) | [bpenteado](https://github.com/bpenteado) | 2023-07-24 | 0.49.1
🗃️ Enable multiple DB access roles in instances (decompose connection string) | [431](https://github.com/laminlabs/lamindb-setup/pull/431) | [bpenteado](https://github.com/bpenteado) | 2023-07-19 | 0.49.0
💚 Fix instability | [441](https://github.com/laminlabs/lamindb-setup/pull/441) | [falexwolf](https://github.com/falexwolf) | 2023-07-19 |
👷 Run CI against the staging environment | [440](https://github.com/laminlabs/lamindb-setup/pull/440) | [bpenteado](https://github.com/bpenteado) | 2023-07-18 |
♻️ Simplify `StorageSettings` | [439](https://github.com/laminlabs/lamindb-setup/pull/439) | [falexwolf](https://github.com/falexwolf) | 2023-07-17 | 0.48.8
🔒️ Increase locker expiration time to 1 week | [437](https://github.com/laminlabs/lamindb-setup/pull/437) | [Koncopd](https://github.com/Koncopd) | 2023-07-08 | 0.48.7
🚸 Import order of schema modules shouldn't matter | [436](https://github.com/laminlabs/lamindb-setup/pull/436) | [falexwolf](https://github.com/falexwolf) | 2023-07-06 | 0.48.6
🚸 Deal with legacy instances | [435](https://github.com/laminlabs/lamindb-setup/pull/435) | [falexwolf](https://github.com/falexwolf) | 2023-07-06 | 0.48.5
🚸 Raise more errors in API when instance is setup | [434](https://github.com/laminlabs/lamindb-setup/pull/434) | [falexwolf](https://github.com/falexwolf) | 2023-07-03 | 0.48.3
🚸 Silence loggers and close instance during init & load | [433](https://github.com/laminlabs/lamindb-setup/pull/433) | [falexwolf](https://github.com/falexwolf) | 2023-07-03 | 0.48.2
🐛 Fix sqlite file not existing in the bucket error | [432](https://github.com/laminlabs/lamindb-setup/pull/432) | [Koncopd](https://github.com/Koncopd) | 2023-06-28 | 0.48.1
🩹 Do not register storage through `set.storage` on the hub | [430](https://github.com/laminlabs/lamindb-setup/pull/430) | [falexwolf](https://github.com/falexwolf) | 2023-06-28 |
🐛 Lock cloud sqlite instances on init | [429](https://github.com/laminlabs/lamindb-setup/pull/429) | [Koncopd](https://github.com/Koncopd) | 2023-06-27 |
👷 Add local hub tests | [427](https://github.com/laminlabs/lamindb-setup/pull/427) | [falexwolf](https://github.com/falexwolf) | 2023-06-26 |
✨ Set s3 region in set.storage | [428](https://github.com/laminlabs/lamindb-setup/pull/428) | [Koncopd](https://github.com/Koncopd) | 2023-06-26 |
➖ Simplify deps | [426](https://github.com/laminlabs/lamindb-setup/pull/426) | [falexwolf](https://github.com/falexwolf) | 2023-06-21 |
✅ Try to trigger error | [424](https://github.com/laminlabs/lamindb-setup/pull/424) | [falexwolf](https://github.com/falexwolf) | 2023-06-20 | 0.47.11
🔧 Back to UTC | [423](https://github.com/laminlabs/lamindb-setup/pull/423) | [falexwolf](https://github.com/falexwolf) | 2023-06-20 |
♻️ Simplify schema validation | [422](https://github.com/laminlabs/lamindb-setup/pull/422) | [falexwolf](https://github.com/falexwolf) | 2023-06-18 | 0.47.10
🎨  Remove `laminhub_rest` from `lamindb_setup` | [421](https://github.com/laminlabs/lamindb-setup/pull/421) | [bpenteado](https://github.com/bpenteado) | 2023-06-16 |
🍱 Fix legacy data migration | [420](https://github.com/laminlabs/lamindb-setup/pull/420) | [falexwolf](https://github.com/falexwolf) | 2023-06-15 | 0.47.9
✨ Add ability to check for migrations | [419](https://github.com/laminlabs/lamindb-setup/pull/419) | [falexwolf](https://github.com/falexwolf) | 2023-06-14 | 0.47.8
🎨 Updated bionty function imports | [418](https://github.com/laminlabs/lamindb-setup/pull/418) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-14 |
♻️ Different migr strategy | [417](https://github.com/laminlabs/lamindb-setup/pull/417) | [falexwolf](https://github.com/falexwolf) | 2023-06-12 | 0.47.7
🍱 Added migration script from legacy instances | [416](https://github.com/laminlabs/lamindb-setup/pull/416) | [falexwolf](https://github.com/falexwolf) | 2023-06-12 | 0.47.5
🔥 Adapt locker to lock entire lamindb session | [415](https://github.com/laminlabs/lamindb-setup/pull/415) | [Koncopd](https://github.com/Koncopd) | 2023-06-11 |
🚑 Only delete bionty sources when bionty is installed | [414](https://github.com/laminlabs/lamindb-setup/pull/414) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-10 | 0.47.4
🚚 Rename source_key to source | [413](https://github.com/laminlabs/lamindb-setup/pull/413) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-10 | 0.47.3
⬆️ Rename bionty variables | [412](https://github.com/laminlabs/lamindb-setup/pull/412) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-10 | 0.47.2
🚑 Removed `LAMINDB_INSTANCE_LOADED` env variable | [411](https://github.com/laminlabs/lamindb-setup/pull/411) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-09 | 0.47.1
⬆️ Adapt to bionty naming in 0.18.0 | [410](https://github.com/laminlabs/lamindb-setup/pull/410) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-09 |
🚸 Warn about migrations | [409](https://github.com/laminlabs/lamindb-setup/pull/409) | [falexwolf](https://github.com/falexwolf) | 2023-06-08 | 0.47.0
📝 Refactor guide | [408](https://github.com/laminlabs/lamindb-setup/pull/408) | [falexwolf](https://github.com/falexwolf) | 2023-06-08 |
✅ More testing of Bionty | [405](https://github.com/laminlabs/lamindb-setup/pull/405) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 | 0.46a5
♻️ Reorder args of CLI | [407](https://github.com/laminlabs/lamindb-setup/pull/407) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 |
♻️ Refactored init & load instance | [406](https://github.com/laminlabs/lamindb-setup/pull/406) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 |
💚 Fix dependencies | [404](https://github.com/laminlabs/lamindb-setup/pull/404) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 |
♻️ Rename and set empty locker for now | [403](https://github.com/laminlabs/lamindb-setup/pull/403) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 |
🚸 Simplify remote SQLite synching & locking | [402](https://github.com/laminlabs/lamindb-setup/pull/402) | [falexwolf](https://github.com/falexwolf) | 2023-06-07 |
✨ Setup bionty version tables | [400](https://github.com/laminlabs/lamindb-setup/pull/400) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-07 |
🔊 Better logging | [399](https://github.com/laminlabs/lamindb-setup/pull/399) | [falexwolf](https://github.com/falexwolf) | 2023-06-05 | 0.46a4
💄 Prettier settings file names | [398](https://github.com/laminlabs/lamindb-setup/pull/398) | [falexwolf](https://github.com/falexwolf) | 2023-06-04 |
⚡ Bionty versions table & performance improvements | [396](https://github.com/laminlabs/lamindb-setup/pull/396) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-04 |
🔥 Remove all occurances of sqlmodel | [397](https://github.com/laminlabs/lamindb-setup/pull/397) | [falexwolf](https://github.com/falexwolf) | 2023-06-04 |
🔥 Delete SQLAlchemy related content | [395](https://github.com/laminlabs/lamindb-setup/pull/395) | [falexwolf](https://github.com/falexwolf) | 2023-06-04 |
🔥 Remove alembic migrations infra & fix coverage | [394](https://github.com/laminlabs/lamindb-setup/pull/394) | [falexwolf](https://github.com/falexwolf) | 2023-06-04 |
✨ Add migrations management for Django (start breaking SQLAlchemy) | [393](https://github.com/laminlabs/lamindb-setup/pull/393) | [falexwolf](https://github.com/falexwolf) | 2023-06-04 |
✨ Extend django to lnschema-bionty | [392](https://github.com/laminlabs/lamindb-setup/pull/392) | [sunnyosun](https://github.com/sunnyosun) | 2023-06-04 |
✨ New id and version in track | [391](https://github.com/laminlabs/lamindb-setup/pull/391) | [Koncopd](https://github.com/Koncopd) | 2023-06-02 |
🏗️ Enable Django backend (lamindb) | [390](https://github.com/laminlabs/lamindb-setup/pull/390) | [falexwolf](https://github.com/falexwolf) | 2023-06-02 |
🚚 Rename package to `lamindb_setup` | [389](https://github.com/laminlabs/lamindb-setup/pull/389) | [falexwolf](https://github.com/falexwolf) | 2023-06-01 | 0.46a2
🏗️ Add Django backend | [388](https://github.com/laminlabs/lndb/pull/388) | [falexwolf](https://github.com/falexwolf) | 2023-05-31 | 0.46a1
♻️ Refactor CI | [387](https://github.com/laminlabs/lndb/pull/387) | [falexwolf](https://github.com/falexwolf) | 2023-05-30 | 0.45.0
🐛 Unlock on uncaught exceptions in ipython | [386](https://github.com/laminlabs/lndb/pull/386) | [Koncopd](https://github.com/Koncopd) | 2023-05-30 |
⬆️ Upgrade to laminhub-rest 0.9.8 | [384](https://github.com/laminlabs/lndb/pull/384) | [falexwolf](https://github.com/falexwolf) | 2023-05-28 | 0.45a4
⬆️ Upgrade laminhub-rest to 0.9.7 | [383](https://github.com/laminlabs/lndb/pull/383) | [falexwolf](https://github.com/falexwolf) | 2023-05-28 | 0.45a3
📝 Add setup overview from lamindb | [382](https://github.com/laminlabs/lndb/pull/382) | [falexwolf](https://github.com/falexwolf) | 2023-05-27 |
🔥 Remove schema modules logic from `setup_schema` | [381](https://github.com/laminlabs/lndb/pull/381) | [falexwolf](https://github.com/falexwolf) | 2023-05-26 | 0.45a2
🏗️ Remove SQL-level schema modules | [380](https://github.com/laminlabs/lndb/pull/380) | [falexwolf](https://github.com/falexwolf) | 2023-05-25 | 0.45a1
✨ Add track command to CLI | [378](https://github.com/laminlabs/lndb/pull/378) | [Koncopd](https://github.com/Koncopd) | 2023-05-23 |
📝 Improve migration docs | [379](https://github.com/laminlabs/lndb/pull/379) | [Zethson](https://github.com/Zethson) | 2023-05-22 |
🔊 Use lamin_logger in test_notebooks.py | [376](https://github.com/laminlabs/lndb/pull/376) | [Koncopd](https://github.com/Koncopd) | 2023-05-04 |
Add universal_pathlib to dependencies | [375](https://github.com/laminlabs/lndb/pull/375) | [Zethson](https://github.com/Zethson) | 2023-05-02 |
⬆️ Upgrade laminhub-rest to 0.9.4 | [373](https://github.com/laminlabs/lndb/pull/373) | [sunnyosun](https://github.com/sunnyosun) | 2023-04-28 | 0.44.7
🚑 Fix load | [372](https://github.com/laminlabs/lndb/pull/372) | [sunnyosun](https://github.com/sunnyosun) | 2023-04-27 | 0.44.6
✨ Add `--storage` arg to `lamin load` | [370](https://github.com/laminlabs/lndb/pull/370) | [falexwolf](https://github.com/falexwolf) | 2023-04-27 |
⬆️ Upgrade laminhub-rest | [369](https://github.com/laminlabs/lndb/pull/369) | [fredericenard](https://github.com/fredericenard) | 2023-04-25 | 0.44.5
✨ Allow to set additional fsspec kwargs for cloud instances | [366](https://github.com/laminlabs/lndb/pull/366) | [Koncopd](https://github.com/Koncopd) | 2023-04-23 | 0.44.4
⬆️ Upgrade laminhub-rest to 0.8.2 | [365](https://github.com/laminlabs/lndb/pull/365) | [falexwolf](https://github.com/falexwolf) | 2023-04-22 | 0.44.2
🚸 New migration deployment logic that also factors in migration ids | [364](https://github.com/laminlabs/lndb/pull/364) | [falexwolf](https://github.com/falexwolf) | 2023-04-21 |
🚸 Mute warning about DB not reachable in init | [363](https://github.com/laminlabs/lndb/pull/363) | [falexwolf](https://github.com/falexwolf) | 2023-04-21 |
🚸 Allow registering local postgres instances on the hub | [361](https://github.com/laminlabs/lndb/pull/361) | [falexwolf](https://github.com/falexwolf) | 2023-04-21 | 0.43.0
🚸 Add a `is_db_setup()` check after init and make it more robust | [362](https://github.com/laminlabs/lndb/pull/362) | [falexwolf](https://github.com/falexwolf) | 2023-04-20 |
👷 Remove laminhub-rest CI calls | [360](https://github.com/laminlabs/lndb/pull/360) | [falexwolf](https://github.com/falexwolf) | 2023-04-20 |
🚸 Enable non-owner to set storage | [358](https://github.com/laminlabs/lndb/pull/358) | [falexwolf](https://github.com/falexwolf) | 2023-04-19 |
♻️ Restructure hub imports | [357](https://github.com/laminlabs/lndb/pull/357) | [falexwolf](https://github.com/falexwolf) | 2023-04-18 |
⬆️ Upgrade to laminhub_rest 0.8.1 | [356](https://github.com/laminlabs/lndb/pull/356) | [falexwolf](https://github.com/falexwolf) | 2023-04-18 | 0.42.0
✅ Use nbproject-test directly | [355](https://github.com/laminlabs/lndb/pull/355) | [Koncopd](https://github.com/Koncopd) | 2023-04-18 |
🔊 Clarify access based on locally cached instance metadata | [354](https://github.com/laminlabs/lndb/pull/354) | [falexwolf](https://github.com/falexwolf) | 2023-04-11 | 0.41.0
🎨 Move setup checks from lamindb here | [352](https://github.com/laminlabs/lndb/pull/352) | [falexwolf](https://github.com/falexwolf) | 2023-04-08 | 0.41rc1
🔧 Increase configure-aws-credentials version from v1 to v2 | [344](https://github.com/laminlabs/lndb/pull/344) | [Zethson](https://github.com/Zethson) | 2023-04-07 |
🚸 Expose `id` in `StorageSettings` | [351](https://github.com/laminlabs/lndb/pull/351) | [falexwolf](https://github.com/falexwolf) | 2023-04-07 | 0.40.2
➕ Add cloudpathlib back | [350](https://github.com/laminlabs/lndb/pull/350) | [falexwolf](https://github.com/falexwolf) | 2023-04-07 | 0.40.1
🚸 Rename settings directory to `.lamin` | [349](https://github.com/laminlabs/lndb/pull/349) | [falexwolf](https://github.com/falexwolf) | 2023-04-07 | 0.40.0
🚸 Expose storage in settings | [348](https://github.com/laminlabs/lndb/pull/348) | [falexwolf](https://github.com/falexwolf) | 2023-04-07 |
♻️ Move storage related code to lndb_storage | [338](https://github.com/laminlabs/lndb/pull/338) | [Koncopd](https://github.com/Koncopd) | 2023-04-05 |
♻️ Refactor migrations | [346](https://github.com/laminlabs/lndb/pull/346) | [falexwolf](https://github.com/falexwolf) | 2023-04-04 |
🧑‍💻 Enable to load instance from hub using access token | [343](https://github.com/laminlabs/lndb/pull/343) | [fredericenard](https://github.com/fredericenard) | 2023-04-03 | 0.39.2
⬆️ Upgrade laminhub-rest to 0.7.2 | [341](https://github.com/laminlabs/lndb/pull/341) | [falexwolf](https://github.com/falexwolf) | 2023-03-31 | 0.39.1
💚 Try to fix CI | [342](https://github.com/laminlabs/lndb/pull/342) | [falexwolf](https://github.com/falexwolf) | 2023-03-30 |
🚸 Check schema version upon init | [340](https://github.com/laminlabs/lndb/pull/340) | [falexwolf](https://github.com/falexwolf) | 2023-03-30 | 0.39.0
🚸 Enforce order of schema modules for migration | [339](https://github.com/laminlabs/lndb/pull/339) | [falexwolf](https://github.com/falexwolf) | 2023-03-27 | 0.38.1
💚 Improve e2e migrations guide | [337](https://github.com/laminlabs/lndb/pull/337) | [falexwolf](https://github.com/falexwolf) | 2023-03-26 | 0.38.0
🚸 Update `_migration` in `__init__.py` of schema modules | [336](https://github.com/laminlabs/lndb/pull/336) | [falexwolf](https://github.com/falexwolf) | 2023-03-25 |
🎨 Improve e2e migrations testing | [335](https://github.com/laminlabs/lndb/pull/335) | [falexwolf](https://github.com/falexwolf) | 2023-03-25 |
🎨 Improve switching logic from and to sqlite | [334](https://github.com/laminlabs/lndb/pull/334) | [falexwolf](https://github.com/falexwolf) | 2023-03-22 | 0.37.9
✅ Add tests for migration unit tests | [333](https://github.com/laminlabs/lndb/pull/333) | [falexwolf](https://github.com/falexwolf) | 2023-03-22 | 0.37.8
🚑 Restore previous criterion to test postgres vs sqlite | [332](https://github.com/laminlabs/lndb/pull/332) | [falexwolf](https://github.com/falexwolf) | 2023-03-22 | 0.37.7
🚸 Also delete current instance settings | [329](https://github.com/laminlabs/lndb/pull/329) | [falexwolf](https://github.com/falexwolf) | 2023-03-21 | 0.37.6
📝 Prettify init instance guide | [331](https://github.com/laminlabs/lndb/pull/331) | [falexwolf](https://github.com/falexwolf) | 2023-03-21 |
💚 Fix CI | [330](https://github.com/laminlabs/lndb/pull/330) | [sunnyosun](https://github.com/sunnyosun) | 2023-03-21 | 0.37.5
⬆️ Migrate to bionty 0.9 | [328](https://github.com/laminlabs/lndb/pull/328) | [sunnyosun](https://github.com/sunnyosun) | 2023-03-20 | 0.37.5rc1
🚸  Handle instance permissions during `load` | [327](https://github.com/laminlabs/lndb/pull/327) | [bpenteado](https://github.com/bpenteado) | 2023-03-14 |
🐛 Fix `._current.yaml` | [326](https://github.com/laminlabs/lndb/pull/326) | [sunnyosun](https://github.com/sunnyosun) | 2023-03-09 | 0.37.4
⬆️ Upgrade to laminhub_rest 0.6.1 | [325](https://github.com/laminlabs/lndb/pull/325) | [falexwolf](https://github.com/falexwolf) | 2023-03-09 | 0.37.3
⬆️ Upgrade lnschema-bionty | [318](https://github.com/laminlabs/lndb/pull/318) | [sunnyosun](https://github.com/sunnyosun) | 2023-03-09 | 0.37.2
✅ Test lamindb_setup in staging environment | [324](https://github.com/laminlabs/lndb/pull/324) | [fredericenard](https://github.com/fredericenard) | 2023-03-08 |
👷 Enable testing using a specific environment | [323](https://github.com/laminlabs/lndb/pull/323) | [fredericenard](https://github.com/fredericenard) | 2023-03-07 |
⬆️ Upgrade laminhub-rest | [322](https://github.com/laminlabs/lndb/pull/322) | [fredericenard](https://github.com/fredericenard) | 2023-03-07 |
⬆️ Upgrade lamindb | [321](https://github.com/laminlabs/lndb/pull/321) | [fredericenard](https://github.com/fredericenard) | 2023-03-07 |
⬆️ Upgrade lamindb | [320](https://github.com/laminlabs/lndb/pull/320) | [fredericenard](https://github.com/fredericenard) | 2023-03-07 | 0.37.1
📌 Pin laminhub-rest | [319](https://github.com/laminlabs/lndb/pull/319) | [fredericenard](https://github.com/fredericenard) | 2023-03-07 |
📝 Reduce visual noise in migrate guide | [317](https://github.com/laminlabs/lndb/pull/317) | [falexwolf](https://github.com/falexwolf) | 2023-03-06 | 0.37.0
📝 Fix docs build | [315](https://github.com/laminlabs/lndb/pull/315) | [Koncopd](https://github.com/Koncopd) | 2023-03-05 |
🐛 Fix resolution of storage root paths for local instances | [314](https://github.com/laminlabs/lndb/pull/314) | [Koncopd](https://github.com/Koncopd) | 2023-03-05 |
📝 Replace `lamin` API with `lamindb_setup` API | [313](https://github.com/laminlabs/lndb/pull/313) | [falexwolf](https://github.com/falexwolf) | 2023-03-01 |
✨ Reload `lamindb.schema` if `lamindb` is imported | [312](https://github.com/laminlabs/lndb/pull/312) | [falexwolf](https://github.com/falexwolf) | 2023-03-01 | 0.36.0
👷 Check laminci versions | [310](https://github.com/laminlabs/lndb/pull/310) | [falexwolf](https://github.com/falexwolf) | 2023-03-01 |
📝 Fix class ref in settings | [308](https://github.com/laminlabs/lndb/pull/308) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-28 |
🚚 Rename lamindb_setup to lamin in guide & faq | [307](https://github.com/laminlabs/lndb/pull/307) | [falexwolf](https://github.com/falexwolf) | 2023-02-27 |
🎨 Rename root_str to root_as_str | [305](https://github.com/laminlabs/lndb/pull/305) | [Koncopd](https://github.com/Koncopd) | 2023-02-25 |
✨ Add root_str property to Storage | [304](https://github.com/laminlabs/lndb/pull/304) | [Koncopd](https://github.com/Koncopd) | 2023-02-25 |
🚚 Rename lamindb_setup CLI to lamin | [303](https://github.com/laminlabs/lndb/pull/303) | [falexwolf](https://github.com/falexwolf) | 2023-02-24 |
🚚 Move code to laminci | [302](https://github.com/laminlabs/lndb/pull/302) | [falexwolf](https://github.com/falexwolf) | 2023-02-23 |
🐛 Fix CI docs upload | [301](https://github.com/laminlabs/lndb/pull/301) | [falexwolf](https://github.com/falexwolf) | 2023-02-22 | 0.35.3
👷 Upload docs artifacts | [300](https://github.com/laminlabs/lndb/pull/300) | [falexwolf](https://github.com/falexwolf) | 2023-02-22 |
👷 Add CI helper for uploading docs | [299](https://github.com/laminlabs/lndb/pull/299) | [falexwolf](https://github.com/falexwolf) | 2023-02-22 | 0.35.2
🐛 Fix trailing slash in storage table | [297](https://github.com/laminlabs/lndb/pull/297) | [falexwolf](https://github.com/falexwolf) | 2023-02-22 | 0.35.1
🐛 Reinit Locker on user or storage change | [296](https://github.com/laminlabs/lndb/pull/296) | [Koncopd](https://github.com/Koncopd) | 2023-02-22 |
🎨 Use resolve() instead of absolute() | [294](https://github.com/laminlabs/lndb/pull/294) | [falexwolf](https://github.com/falexwolf) | 2023-02-21 |
🔧 Parse config for CI to retrieve anon key | [293](https://github.com/laminlabs/lndb/pull/293) | [fredericenard](https://github.com/fredericenard) | 2023-02-21 | 0.35.0
✨ Enable setup of local auth instance | [292](https://github.com/laminlabs/lndb/pull/292) | [fredericenard](https://github.com/fredericenard) | 2023-02-20 | 0.35rc2
⚡️ Replace `CloudPath` with `UPath` | [291](https://github.com/laminlabs/lndb/pull/291) | [Koncopd](https://github.com/Koncopd) | 2023-02-18 |
💄 Fix styling of migrate guide | [290](https://github.com/laminlabs/lndb/pull/290) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-17 |
📝 Polish migration guide | [289](https://github.com/laminlabs/lndb/pull/289) | [falexwolf](https://github.com/falexwolf) | 2023-02-17 |
🐛 Fix nox | [288](https://github.com/laminlabs/lndb/pull/288) | [falexwolf](https://github.com/falexwolf) | 2023-02-16 | 0.34.1
🎨 Deprecate `set_storage` in favor of `set.storage` | [287](https://github.com/laminlabs/lndb/pull/287) | [falexwolf](https://github.com/falexwolf) | 2023-02-16 | 0.34.0
🎨 Introduce `.dev` module | [286](https://github.com/laminlabs/lndb/pull/286) | [falexwolf](https://github.com/falexwolf) | 2023-02-16 |
✏️ Fix example | [285](https://github.com/laminlabs/lndb/pull/285) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-16 |
🚸 Proper client server check | [284](https://github.com/laminlabs/lndb/pull/284) | [falexwolf](https://github.com/falexwolf) | 2023-02-16 | 0.33.0
📝 Migration guide | [283](https://github.com/laminlabs/lndb/pull/283) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-16 |
⬆️ Upgrade hub to 0.4.0 | [282](https://github.com/laminlabs/lndb/pull/282) | [falexwolf](https://github.com/falexwolf) | 2023-02-15 | 0.32.6
🔧 Add `LAMINDB_INSTANCE_LOADED` env variable on load/close | [280](https://github.com/laminlabs/lndb/pull/280) | [Zethson](https://github.com/Zethson) | 2023-02-15 | 0.32.5
♻️ Adapt migration_id test for hub | [281](https://github.com/laminlabs/lndb/pull/281) | [falexwolf](https://github.com/falexwolf) | 2023-02-15 |
🐛 Also update dev-level ORMs | [279](https://github.com/laminlabs/lndb/pull/279) | [falexwolf](https://github.com/falexwolf) | 2023-02-15 |
🔥 Remove synchronization warnings mute in setup_schema | [278](https://github.com/laminlabs/lndb/pull/278) | [Koncopd](https://github.com/Koncopd) | 2023-02-15 |
🐛 Fix a synchronization issue during init of remote sqlite | [277](https://github.com/laminlabs/lndb/pull/277) | [falexwolf](https://github.com/falexwolf) | 2023-02-15 |
🐛 Fix error raising for `get_package_info` | [276](https://github.com/laminlabs/lndb/pull/276) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.4
💚 Fix test_migrate_clones_postgres | [275](https://github.com/laminlabs/lndb/pull/275) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.3
🚑 Generate all migrate files before migrate | [274](https://github.com/laminlabs/lndb/pull/274) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.2
🚑 Fix import and simplify params | [273](https://github.com/laminlabs/lndb/pull/273) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.1
⬆️ Upgrade `laminhub_rest` to 0.3.2 | [272](https://github.com/laminlabs/lndb/pull/272) | [bpenteado](https://github.com/bpenteado) | 2023-02-13 |
🚚 Rename package from `lndb_setup` to `lndb` | [270](https://github.com/laminlabs/lndb/pull/270) | [falexwolf](https://github.com/falexwolf) | 2023-02-12 | 0.32.0
🎨 Enable using the current instance for generating migration script | [271](https://github.com/laminlabs/lndb-setup/pull/271) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 | 0.31.0
🎨 Generate alembic.ini before before check migrate | [269](https://github.com/laminlabs/lndb-setup/pull/269) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 |
✨ Added migration module | [266](https://github.com/laminlabs/lndb-setup/pull/266) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 |
🐛 Fix access to newly created instances on s3 due to region | [268](https://github.com/laminlabs/lndb-setup/pull/268) | [Koncopd](https://github.com/Koncopd) | 2023-02-09 |
🚸 Validate instances at `init` | [264](https://github.com/laminlabs/lndb-setup/pull/264) | [falexwolf](https://github.com/falexwolf) | 2023-02-09 |
🚸 Add error if client version schema package version is lower than deployed db schema module version | [267](https://github.com/laminlabs/lndb-setup/pull/267) | [falexwolf](https://github.com/falexwolf) | 2023-02-09 |
♻️ Remove _sbclient suffix | [263](https://github.com/laminlabs/lndb-setup/pull/263) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 |
🚸 Better UX `delete()` | [260](https://github.com/laminlabs/lndb-setup/pull/260) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 |
🐛 Fix check_versions | [262](https://github.com/laminlabs/lndb-setup/pull/262) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 | 0.30.14
🚑 Fix login on new environments | [261](https://github.com/laminlabs/lndb-setup/pull/261) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 | 0.30.13
🐛 Fix modified for gcs for locker | [258](https://github.com/laminlabs/lndb-setup/pull/258) | [Koncopd](https://github.com/Koncopd) | 2023-02-05 |
✨ Add rich string representation for InstanceSettings class | [254](https://github.com/laminlabs/lndb-setup/pull/254) | [bpenteado](https://github.com/bpenteado) | 2023-01-31 | 0.30.12
✨ Add rich string representation for UserSettings class | [255](https://github.com/laminlabs/lndb-setup/pull/255) | [bpenteado](https://github.com/bpenteado) | 2023-01-31 |
📌 Pin deps for aiobotocore to fix resolution | [commit](https://github.com/laminlabs/lndb-setup/commit/439898124d09f7cab323a1ef275e72eba072a4d7) | [Koncopd](https://github.com/Koncopd) | 2023-01-27 | 0.30.11
⬆️ Upgrade laminhub-rest to 0.1.4 | [252](https://github.com/laminlabs/lndb-setup/pull/252) | [fredericenard](https://github.com/fredericenard) | 2023-01-27 |
⬆️ Cleaned up dependencies | [253](https://github.com/laminlabs/lndb-setup/pull/253) | [sunnyosun](https://github.com/sunnyosun) | 2023-01-27 |
📌 Pin s3fs and gcsfs to the latest versions | [commit](https://github.com/laminlabs/lndb-setup/commit/2aaec804492fd5339c80bfb5b6f8d79ba8117e6e) | [Koncopd](https://github.com/Koncopd) | 2023-01-25 | 0.30.10
🐛 Use `schema_lookup_name` for SQL level | [250](https://github.com/laminlabs/lndb-setup/pull/250) | [falexwolf](https://github.com/falexwolf) | 2023-01-24 | 0.30.9
🦺 Additional safety measures for locker | [249](https://github.com/laminlabs/lndb-setup/pull/249) | [Koncopd](https://github.com/Koncopd) | 2023-01-24 |
🐛 Pin s3fs version | [242](https://github.com/laminlabs/lndb-setup/pull/242) | [fredericenard](https://github.com/fredericenard) | 2023-01-23 | 0.30.8
♻️ Refactor locker and change locking logic to explicit locks | [241](https://github.com/laminlabs/lndb-setup/pull/241) | [Koncopd](https://github.com/Koncopd) | 2023-01-21 |
✅ Delete instances created during failed tests | [240](https://github.com/laminlabs/lndb-setup/pull/240) | [fredericenard](https://github.com/fredericenard) | 2023-01-21 |
🚸 Fix messing with cached user settings upon failed signup | [238](https://github.com/laminlabs/lndb-setup/pull/238) | [falexwolf](https://github.com/falexwolf) | 2023-01-20 | 0.30.7
🩹 Fix creation of storage dir upon re-init | [236](https://github.com/laminlabs/lndb-setup/pull/236) | [falexwolf](https://github.com/falexwolf) | 2023-01-20 |
📝 Move load guide to the front | [235](https://github.com/laminlabs/lndb-setup/pull/235) | [falexwolf](https://github.com/falexwolf) | 2023-01-20 |
🐛 Restore sqlite db synchronization on `instance.session()` call | [233](https://github.com/laminlabs/lndb-setup/pull/233) | [Koncopd](https://github.com/Koncopd) | 2023-01-16 |
🐛 Fix migration config | [232](https://github.com/laminlabs/lndb-setup/pull/232) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.6
🎨 Document ontology versioning and remove legacy sql | [231](https://github.com/laminlabs/lndb-setup/pull/231) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.5
✅ Test schema modules on postgres | [229](https://github.com/laminlabs/lndb-setup/pull/229) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 |
🐛 Fix another occurence | [228](https://github.com/laminlabs/lndb-setup/pull/228) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.4
🔧 Fix nox for testing migrations | [227](https://github.com/laminlabs/lndb-setup/pull/227) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.3
⬆️ Upgrade laminhub-rest | [225](https://github.com/laminlabs/lndb-setup/pull/225) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.2
🔥 Remove atexit | [224](https://github.com/laminlabs/lndb-setup/pull/224) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 |
⬆️ Upgrade laminhub_rest | [223](https://github.com/laminlabs/lndb-setup/pull/223) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.1
🏗️ Separate hub components out to turn lndb-setup into pure client | [220](https://github.com/laminlabs/lndb-setup/pull/220) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.0
🚚 Rename `_setup_instance` to `_init_instance` | [219](https://github.com/laminlabs/lndb-setup/pull/219) | [falexwolf](https://github.com/falexwolf) | 2023-01-14 |
🐛 Fix hub interaction | [218](https://github.com/laminlabs/lndb-setup/pull/218) | [falexwolf](https://github.com/falexwolf) | 2023-01-14 |
🩹 Patch cloning | [217](https://github.com/laminlabs/lndb-setup/pull/217) | [falexwolf](https://github.com/falexwolf) | 2023-01-13 | 0.30a1
🐛 Prevent initializing an instance with a db already used  | [214](https://github.com/laminlabs/lndb-setup/pull/214) | [fredericenard](https://github.com/fredericenard) | 2023-01-13 |
🥅 Properly check if an instance exists during init  | [215](https://github.com/laminlabs/lndb-setup/pull/215) | [fredericenard](https://github.com/fredericenard) | 2023-01-13 |
♻️ Refactor storage property | [213](https://github.com/laminlabs/lndb-setup/pull/213) | [fredericenard](https://github.com/fredericenard) | 2023-01-12 |
🎨 Refactor dialect property | [212](https://github.com/laminlabs/lndb-setup/pull/212) | [fredericenard](https://github.com/fredericenard) | 2023-01-12 |
🐛 Fix checking for non remote instance loaded from hub | [211](https://github.com/laminlabs/lndb-setup/pull/211) | [fredericenard](https://github.com/fredericenard) | 2023-01-12 |
🐛 Prevent loading non remote instance from hub | [209](https://github.com/laminlabs/lndb-setup/pull/209) | [fredericenard](https://github.com/fredericenard) | 2023-01-12 |
🐛 Prevent loading non remote instance from hub | [208](https://github.com/laminlabs/lndb-setup/pull/208) | [fredericenard](https://github.com/fredericenard) | 2023-01-12 |
🐛 Fix `schema=None` | [207](https://github.com/laminlabs/lndb-setup/pull/207) | [Koncopd](https://github.com/Koncopd) | 2023-01-11 |
🎨 Simplify loading | [204](https://github.com/laminlabs/lndb-setup/pull/204) | [fredericenard](https://github.com/fredericenard) | 2023-01-11 |
🗃️ Stop tracking non remote instance in hub | [205](https://github.com/laminlabs/lndb-setup/pull/205) | [fredericenard](https://github.com/fredericenard) | 2023-01-11 |
⬆️ Upgrade lnschema core | [198](https://github.com/laminlabs/lndb-setup/pull/198) | [fredericenard](https://github.com/fredericenard) | 2023-01-10 | 0.29.0
✅ Ensure each deleted instance name are specific to a CI run | [200](https://github.com/laminlabs/lndb-setup/pull/200) | [fredericenard](https://github.com/fredericenard) | 2023-01-10 |
✅  Ensure instances deleted during tests are not used anywhere else | [195](https://github.com/laminlabs/lndb-setup/pull/195) | [fredericenard](https://github.com/fredericenard) | 2023-01-10 |
⬇️ Downgrade lnschema-core | [197](https://github.com/laminlabs/lndb-setup/pull/197) | [fredericenard](https://github.com/fredericenard) | 2023-01-10 |
🐛 Load settings for delete an instance if settings file cannot be found locally | [192](https://github.com/laminlabs/lndb-setup/pull/192) | [fredericenard](https://github.com/fredericenard) | 2023-01-09 |
✨ Load remote instance | [174](https://github.com/laminlabs/lndb-setup/pull/174) | [fredericenard](https://github.com/fredericenard) | 2023-01-09 |
♻️ Refactor delete function | [191](https://github.com/laminlabs/lndb-setup/pull/191) | [fredericenard](https://github.com/fredericenard) | 2023-01-06 |
🐛 Add schema field to instance metadata | [189](https://github.com/laminlabs/lndb-setup/pull/189) | [fredericenard](https://github.com/fredericenard) | 2023-01-06 |
🐛 Make delete command compatible with delete function | [188](https://github.com/laminlabs/lndb-setup/pull/188) | [fredericenard](https://github.com/fredericenard) | 2023-01-06 |
✨ Command delete for the CLI | [175](https://github.com/laminlabs/lndb-setup/pull/175) | [fredericenard](https://github.com/fredericenard) | 2023-01-05 |
🍱 Clean up & complete test instances | [186](https://github.com/laminlabs/lndb-setup/pull/186) | [falexwolf](https://github.com/falexwolf) | 2023-01-05 | 0.28.1
✨ Output information about current user in `lamindb_setup info` | [185](https://github.com/laminlabs/lndb-setup/pull/185) | [falexwolf](https://github.com/falexwolf) | 2023-01-05 |
✨ Add command for generating migrations | [184](https://github.com/laminlabs/lndb-setup/pull/184) | [falexwolf](https://github.com/falexwolf) | 2023-01-05 |
📝 Added swarm_test instance | [183](https://github.com/laminlabs/lndb-setup/pull/183) | [sunnyosun](https://github.com/sunnyosun) | 2023-01-05 |
🐛 Migration skipped return None | [181](https://github.com/laminlabs/lndb-setup/pull/181) | [fredericenard](https://github.com/fredericenard) | 2023-01-05 | 0.28.0
📝 Add a clone guide | [179](https://github.com/laminlabs/lndb-setup/pull/179) | [falexwolf](https://github.com/falexwolf) | 2023-01-04 |
🚸 Less dangerous error message for deleted remote sqlite file | [180](https://github.com/laminlabs/lndb-setup/pull/180) | [falexwolf](https://github.com/falexwolf) | 2023-01-04 |
🍱 Add hub schema | [178](https://github.com/laminlabs/lndb-setup/pull/178) | [falexwolf](https://github.com/falexwolf) | 2023-01-04 |
✨ Enable to specify name when setup a local test postgres | [177](https://github.com/laminlabs/lndb-setup/pull/177) | [fredericenard](https://github.com/fredericenard) | 2023-01-04 |
✨ Mutual exclusion for a sqlite db file | [95](https://github.com/laminlabs/lndb-setup/pull/95) | [Koncopd](https://github.com/Koncopd) | 2022-12-27 |
📝 Move `set_storage` guide to the front | [172](https://github.com/laminlabs/lndb-setup/pull/172) | [falexwolf](https://github.com/falexwolf) | 2022-12-22 |
🎨 Remove implicit casting to boolean | [171](https://github.com/laminlabs/lndb-setup/pull/171) | [fredericenard](https://github.com/fredericenard) | 2022-12-20 | 0.27.0
🐛 Fix CLI `info` and `set` | [169](https://github.com/laminlabs/lndb-setup/pull/169) | [falexwolf](https://github.com/falexwolf) | 2022-12-19 |
🎨 Refactor push instance function | [168](https://github.com/laminlabs/lndb-setup/pull/168) | [fredericenard](https://github.com/fredericenard) | 2022-12-17 |
🎨 Add owner handle in instance settings file name | [167](https://github.com/laminlabs/lndb-setup/pull/167) | [fredericenard](https://github.com/fredericenard) | 2022-12-17 |
🐛 Catch all cloudpath & client occurences for gs | [166](https://github.com/laminlabs/lndb-setup/pull/166) | [falexwolf](https://github.com/falexwolf) | 2022-12-16 | 0.26.3
🎨 Alleviate inconsistency in `set_storage` | [163](https://github.com/laminlabs/lndb-setup/pull/163) | [falexwolf](https://github.com/falexwolf) | 2022-12-15 | 0.26.2
🎨 Cache owner | [162](https://github.com/laminlabs/lndb-setup/pull/162) | [falexwolf](https://github.com/falexwolf) | 2022-12-15 |
♻️ Refactor set & show | [160](https://github.com/laminlabs/lndb-setup/pull/160) | [falexwolf](https://github.com/falexwolf) | 2022-12-15 | 0.26.1
🚸 Enable to work with gcloud login | [157](https://github.com/laminlabs/lndb-setup/pull/157) | [falexwolf](https://github.com/falexwolf) | 2022-12-15 | 0.26.0
✨ Add info command to CLI | [159](https://github.com/laminlabs/lndb-setup/pull/159) | [fredericenard](https://github.com/fredericenard) | 2022-12-15 |
🚸 Improve logging for instance initialization | [158](https://github.com/laminlabs/lndb-setup/pull/158) | [fredericenard](https://github.com/fredericenard) | 2022-12-15 |
📝 Create FAQ | [147](https://github.com/laminlabs/lndb-setup/pull/147) | [fredericenard](https://github.com/fredericenard) | 2022-12-15 |
✨ Enable to switch storage for postgres | [155](https://github.com/laminlabs/lndb-setup/pull/155) | [fredericenard](https://github.com/fredericenard) | 2022-12-15 |
👷 Remove `%time` command to allow tests to fail | [156](https://github.com/laminlabs/lndb-setup/pull/156) | [falexwolf](https://github.com/falexwolf) | 2022-12-15 |
🐛 Fix migrations e2e testing | [154](https://github.com/laminlabs/lndb-setup/pull/154) | [falexwolf](https://github.com/falexwolf) | 2022-12-14 | 0.25.3
🩹 Fix sync warnings on instance creation | [152](https://github.com/laminlabs/lndb-setup/pull/152) | [Koncopd](https://github.com/Koncopd) | 2022-12-13 |
📝 Improve CLI init docs | [151](https://github.com/laminlabs/lndb-setup/pull/151) | [falexwolf](https://github.com/falexwolf) | 2022-12-13 |
🐛 Fix dbconfig | [150](https://github.com/laminlabs/lndb-setup/pull/150) | [falexwolf](https://github.com/falexwolf) | 2022-12-13 | 0.25.2
✅ Add test for postgres | [149](https://github.com/laminlabs/lndb-setup/pull/149) | [falexwolf](https://github.com/falexwolf) | 2022-12-13 |
🐛 Fix missing return | [148](https://github.com/laminlabs/lndb-setup/pull/148) | [fredericenard](https://github.com/fredericenard) | 2022-12-13 | 0.25.1
🎨 Infer db file name from instance name | [144](https://github.com/laminlabs/lndb-setup/pull/144) | [fredericenard](https://github.com/fredericenard) | 2022-12-13 |
🎨 Drop name property | [143](https://github.com/laminlabs/lndb-setup/pull/143) | [fredericenard](https://github.com/fredericenard) | 2022-12-13 |
✅ Test google cloud bucket | [145](https://github.com/laminlabs/lndb-setup/pull/145) | [falexwolf](https://github.com/falexwolf) | 2022-12-13 |
🚸 Persist instance name during init | [142](https://github.com/laminlabs/lndb-setup/pull/142) | [fredericenard](https://github.com/fredericenard) | 2022-12-13 |
✅ Add a new test for custom instance name | [140](https://github.com/laminlabs/lndb-setup/pull/140) | [fredericenard](https://github.com/fredericenard) | 2022-12-12 |
🎨 Instance initialization with url argument | [138](https://github.com/laminlabs/lndb-setup/pull/138) | [fredericenard](https://github.com/fredericenard) | 2022-12-12 |
👷 Check whether scripts dir exists | [137](https://github.com/laminlabs/lndb-setup/pull/137) | [falexwolf](https://github.com/falexwolf) | 2022-12-12 |
👷 Extend nox | [136](https://github.com/laminlabs/lndb-setup/pull/136) | [falexwolf](https://github.com/falexwolf) | 2022-12-12 | 0.25.0
🐛 Restore synchronization of the sqlite db file | [133](https://github.com/laminlabs/lndb-setup/pull/133) | [Koncopd](https://github.com/Koncopd) | 2022-12-11 |
✅ Test for custom instance name | [134](https://github.com/laminlabs/lndb-setup/pull/134) | [fredericenard](https://github.com/fredericenard) | 2022-12-11 |
✨ Enable to specify name during instance initialization | [131](https://github.com/laminlabs/lndb-setup/pull/131) | [fredericenard](https://github.com/fredericenard) | 2022-12-10 |
🎨 Simplify nox tools | [132](https://github.com/laminlabs/lndb-setup/pull/132) | [falexwolf](https://github.com/falexwolf) | 2022-12-09 | 0.24.4
🚸 Better error logging login | [129](https://github.com/laminlabs/lndb-setup/pull/129) | [falexwolf](https://github.com/falexwolf) | 2022-12-08 | 0.24.3
🐛 Fix typo | [128](https://github.com/laminlabs/lndb-setup/pull/128) | [falexwolf](https://github.com/falexwolf) | 2022-12-08 | 0.24.2
👷 Configure test instance with schemas | [127](https://github.com/laminlabs/lndb-setup/pull/127) | [falexwolf](https://github.com/falexwolf) | 2022-12-08 | 0.24.1
✨ Added bionty versioning | [121](https://github.com/laminlabs/lndb-setup/pull/121) | [sunnyosun](https://github.com/sunnyosun) | 2022-12-08 | 0.24.0
🎨 Add more nox utilities | [126](https://github.com/laminlabs/lndb-setup/pull/126) | [falexwolf](https://github.com/falexwolf) | 2022-12-08 |
♻️ Clean up migration testing framework | [125](https://github.com/laminlabs/lndb-setup/pull/125) | [falexwolf](https://github.com/falexwolf) | 2022-12-08 | 0.23.1
✨ Enable settings to be specific for an environment | [120](https://github.com/laminlabs/lndb-setup/pull/120) | [fredericenard](https://github.com/fredericenard) | 2022-12-08 | 0.23.0
🎨 Drop dynamic settings logic | [124](https://github.com/laminlabs/lndb-setup/pull/124) | [fredericenard](https://github.com/fredericenard) | 2022-12-08 |
✨ Expand `model_definitions_match_ddl` for postgres & add nox tools | [123](https://github.com/laminlabs/lndb-setup/pull/123) | [falexwolf](https://github.com/falexwolf) | 2022-12-07 | 0.22.2
💚 Fix migration & additional testing primitive | [122](https://github.com/laminlabs/lndb-setup/pull/122) | [falexwolf](https://github.com/falexwolf) | 2022-12-07 | 0.22.1
✨ Add a close command | [119](https://github.com/laminlabs/lndb-setup/pull/119) | [falexwolf](https://github.com/falexwolf) | 2022-12-05 | 0.22.0
🎨 Refactor `InstanceSettings.schema` | [118](https://github.com/laminlabs/lndb-setup/pull/118) | [falexwolf](https://github.com/falexwolf) | 2022-12-05 |
🐛 Update site base url for test env | [117](https://github.com/laminlabs/lndb-setup/pull/117) | [fredericenard](https://github.com/fredericenard) | 2022-12-03 | 0.21.0
🎨 More clarity around schema names | [116](https://github.com/laminlabs/lndb-setup/pull/116) | [falexwolf](https://github.com/falexwolf) | 2022-11-29 |
♻️ Refactor assets | [114](https://github.com/laminlabs/lndb-setup/pull/114) | [falexwolf](https://github.com/falexwolf) | 2022-11-29 |
🐛 Fix return codes CLI | [113](https://github.com/laminlabs/lndb-setup/pull/113) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 |
⬆️ Upgrade `lnschema_core` | [112](https://github.com/laminlabs/lndb-setup/pull/112) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 | 0.20.4
🐛 Propagate migrate | [111](https://github.com/laminlabs/lndb-setup/pull/111) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 | 0.20.3
🚸 Reload schemas when switching between sqlite & postgres | [110](https://github.com/laminlabs/lndb-setup/pull/110) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 | 0.20.2
♻️ Refactor `init`, `load`, and `InstanceSettings` | [109](https://github.com/laminlabs/lndb-setup/pull/109) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 |
🎨 Improve migration testing framework | [108](https://github.com/laminlabs/lndb-setup/pull/108) | [falexwolf](https://github.com/falexwolf) | 2022-11-28 | 0.20.1
✨ Migrations testing framework | [107](https://github.com/laminlabs/lndb-setup/pull/107) | [falexwolf](https://github.com/falexwolf) | 2022-11-27 | 0.20.0
⬆️ Upgrade `lnschema_core` | [106](https://github.com/laminlabs/lndb-setup/pull/106) | [fredericenard](https://github.com/fredericenard) | 2022-11-27 | 0.19.0
🍱 Clone postgres to test DB | [104](https://github.com/laminlabs/lndb-setup/pull/104) | [falexwolf](https://github.com/falexwolf) | 2022-11-26 |
📝 Update docstring | [105](https://github.com/laminlabs/lndb-setup/pull/105) | [fredericenard](https://github.com/fredericenard) | 2022-11-26 |
✨ Add clone capability for testing migrations | [103](https://github.com/laminlabs/lndb-setup/pull/103) | [falexwolf](https://github.com/falexwolf) | 2022-11-25 |
✨ Enable dynamic connector file | [100](https://github.com/laminlabs/lndb-setup/pull/100) | [fredericenard](https://github.com/fredericenard) | 2022-11-24 |
🐛 Look for a unique instance name by owner | [101](https://github.com/laminlabs/lndb-setup/pull/101) | [fredericenard](https://github.com/fredericenard) | 2022-11-24 |
✨ Add `session()` to `InstanceSettings` | [99](https://github.com/laminlabs/lndb-setup/pull/99) | [falexwolf](https://github.com/falexwolf) | 2022-11-23 | 0.18.0
✨ Multi environments setup | [98](https://github.com/laminlabs/lndb-setup/pull/98) | [fredericenard](https://github.com/fredericenard) | 2022-11-23 | 0.17.0
✨ Enable to skip migration | [97](https://github.com/laminlabs/lndb-setup/pull/97) | [fredericenard](https://github.com/fredericenard) | 2022-11-19 | 0.16.0
🔥 Do not enforce foreign key integrity on sqlite | [96](https://github.com/laminlabs/lndb-setup/pull/96) | [falexwolf](https://github.com/falexwolf) | 2022-11-14 | 0.15.0
🚸 Fixed warning | [94](https://github.com/laminlabs/lndb-setup/pull/94) | [falexwolf](https://github.com/falexwolf) | 2022-11-03 | 0.14.1
💚 Fix static lnschema_core import | [93](https://github.com/laminlabs/lndb-setup/pull/93) | [falexwolf](https://github.com/falexwolf) | 2022-11-03 | 0.14.0
✨ Modularize SQL schema & camel-case Python table classes | [92](https://github.com/laminlabs/lndb-setup/pull/92) | [falexwolf](https://github.com/falexwolf) | 2022-11-03 |
🩹 Hide hub error in a thread on Windows | [90](https://github.com/laminlabs/lndb-setup/pull/90) | [Koncopd](https://github.com/Koncopd) | 2022-10-30 |
🐛 Add entry in user_instance table | [89](https://github.com/laminlabs/lndb-setup/pull/89) | [fredericenard](https://github.com/fredericenard) | 2022-10-27 | 0.13.2
🐛 Fix instance exists check | [88](https://github.com/laminlabs/lndb-setup/pull/88) | [fredericenard](https://github.com/fredericenard) | 2022-10-26 | 0.13.1
🔥 Remove lamindb_setup hub depency | [86](https://github.com/laminlabs/lndb-setup/pull/86) | [fredericenard](https://github.com/fredericenard) | 2022-10-26 | 0.13.0
🎨 Remove lndb_hub dependency | [85](https://github.com/laminlabs/lndb-setup/pull/85) | [fredericenard](https://github.com/fredericenard) | 2022-10-26 |
✨ Enable dev settings | [84](https://github.com/laminlabs/lndb-setup/pull/84) | [fredericenard](https://github.com/fredericenard) | 2022-10-26 |
✨ Push instance during setup | [81](https://github.com/laminlabs/lndb-setup/pull/81) | [fredericenard](https://github.com/fredericenard) | 2022-10-22 |
🐛 Skip migration if None | [80](https://github.com/laminlabs/lndb-setup/pull/80) | [falexwolf](https://github.com/falexwolf) | 2022-10-20 | 0.12.2
🩹 Fix password error message at login | [79](https://github.com/laminlabs/lndb-setup/pull/79) | [bpenteado](https://github.com/bpenteado) | 2022-10-20 |
✨ Enable to bypass instance settings file | [72](https://github.com/laminlabs/lndb-setup/pull/72) | [fredericenard](https://github.com/fredericenard) | 2022-10-18 |
✨ Store access token | [76](https://github.com/laminlabs/lndb-setup/pull/76) | [fredericenard](https://github.com/fredericenard) | 2022-10-17 |
🐛 Fix sqlite file update | [74](https://github.com/laminlabs/lndb-setup/pull/74) | [falexwolf](https://github.com/falexwolf) | 2022-10-12 | 0.12.1
🚸 Only write migration version upon schema creation | [71](https://github.com/laminlabs/lndb-setup/pull/71) | [falexwolf](https://github.com/falexwolf) | 2022-10-12 |
🚸 Enforce foreign key constraints in SQLite | [70](https://github.com/laminlabs/lndb-setup/pull/70) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 | 0.12.0
📝 Overhaul docs | [69](https://github.com/laminlabs/lndb-setup/pull/69) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
🚸 Fixed migration retrieval warning | [67](https://github.com/laminlabs/lndb-setup/pull/67) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 | 0.11.0
🐛 Populate user name | [66](https://github.com/laminlabs/lndb-setup/pull/66) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
✨ Add drylab schema | [63](https://github.com/laminlabs/lndb-setup/pull/63) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
🚚 Rename `storage_dir` to `storage_root` | [65](https://github.com/laminlabs/lndb-setup/pull/65) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
✨ Add user name to settings | [64](https://github.com/laminlabs/lndb-setup/pull/64) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
🐛 Fix incompatibility with postgres | [61](https://github.com/laminlabs/lndb-setup/pull/61) | [fredericenard](https://github.com/fredericenard) | 2022-10-10 | 0.10.1
✨ Track user name | [62](https://github.com/laminlabs/lndb-setup/pull/62) | [falexwolf](https://github.com/falexwolf) | 2022-10-10 |
🐛 Redirect user after signup | [60](https://github.com/laminlabs/lndb-setup/pull/60) | [fredericenard](https://github.com/fredericenard) | 2022-10-07 | 0.10.0
🐛 Fix storage not inserted during instance setup | [59](https://github.com/laminlabs/lndb-setup/pull/59) | [fredericenard](https://github.com/fredericenard) | 2022-10-07 |
🩹 Cast base settings dir as Path | [57](https://github.com/laminlabs/lndb-setup/pull/57) | [fredericenard](https://github.com/fredericenard) | 2022-10-04 | 0.9.4
🏗️ Enable other settings location | [56](https://github.com/laminlabs/lndb-setup/pull/56) | [fredericenard](https://github.com/fredericenard) | 2022-10-04 | 0.9.2
🔊 Better logging during instance creation | [55](https://github.com/laminlabs/lndb-setup/pull/55) | [falexwolf](https://github.com/falexwolf) | 2022-10-03 |
🐛 Ensure compat with lnbfx | [53](https://github.com/laminlabs/lndb-setup/pull/53) | [falexwolf](https://github.com/falexwolf) | 2022-10-03 | 0.9.1
✨ Implement migrations for all schema modules | [52](https://github.com/laminlabs/lndb-setup/pull/52) | [falexwolf](https://github.com/falexwolf) | 2022-10-03 | 0.9.0
✨ Add docking schema module | [51](https://github.com/laminlabs/lndb-setup/pull/51) | [falexwolf](https://github.com/falexwolf) | 2022-10-03 |
🏗️ Make setup compatible with postgres | [50](https://github.com/laminlabs/lndb-setup/pull/50) | [fredericenard](https://github.com/fredericenard) | 2022-10-03 | 0.8.3
🔥 Removed `configure_schema_wetlab` | [47](https://github.com/laminlabs/lndb-setup/pull/47) | [sunnyosun](https://github.com/sunnyosun) | 2022-09-28 | 0.8.2
🚑 Fix CLI load | [commit](https://github.com/laminlabs/lndb-setup/commit/1ca92fd5c340da101b759d4e3d687982dd0338ef) | [falexwolf](https://github.com/falexwolf) | 2022-09-26 | 0.8.1
🚸 Make load arg positional | [46](https://github.com/laminlabs/lndb-setup/pull/46) | [falexwolf](https://github.com/falexwolf) | 2022-09-26 | 0.8.0
🎨 Explicitly treat no schema modules as None | [45](https://github.com/laminlabs/lndb-setup/pull/45) | [falexwolf](https://github.com/falexwolf) | 2022-09-24 |
🚸 Do not error if local file is newer, print warning instead | [44](https://github.com/laminlabs/lndb-setup/pull/44) | [falexwolf](https://github.com/falexwolf) | 2022-09-24 |
🎨 Simplify version selection | [43](https://github.com/laminlabs/lndb-setup/pull/43) | [falexwolf](https://github.com/falexwolf) | 2022-09-24 |
✅ Test bionty migration | [42](https://github.com/laminlabs/lndb-setup/pull/42) | [falexwolf](https://github.com/falexwolf) | 2022-09-22 | 0.7.1
✨ Track versions and migrations of bionty | [41](https://github.com/laminlabs/lndb-setup/pull/41) | [falexwolf](https://github.com/falexwolf) | 2022-09-22 |
🚚 Rename `user_settings` to `usettings` | [40](https://github.com/laminlabs/lndb-setup/pull/40) | [falexwolf](https://github.com/falexwolf) | 2022-09-21 | 0.7.0
✨ Automatic migration for core schema module | [39](https://github.com/laminlabs/lndb-setup/pull/39) | [falexwolf](https://github.com/falexwolf) | 2022-09-21 |
✨ Populate migration upon instance init | [38](https://github.com/laminlabs/lndb-setup/pull/38) | [falexwolf](https://github.com/falexwolf) | 2022-09-18 |
🍱 Added swarm biosample schema | [36](https://github.com/laminlabs/lndb-setup/pull/36) | [sunnyosun](https://github.com/sunnyosun) | 2022-09-08 | 0.6.3
✅ Add GCP test bucket | [35](https://github.com/laminlabs/lndb-setup/pull/35) | [falexwolf](https://github.com/falexwolf) | 2022-09-06 |
🚸 Call out storage location `us-east-1` and re-factor | [34](https://github.com/laminlabs/lndb-setup/pull/34) | [falexwolf](https://github.com/falexwolf) | 2022-09-05 |
✨ Automatically add storage region | [33](https://github.com/laminlabs/lndb-setup/pull/33) | [falexwolf](https://github.com/falexwolf) | 2022-09-05 | 0.6.2
🚚 Rename guides to guide | [32](https://github.com/laminlabs/lndb-setup/pull/32) | [falexwolf](https://github.com/falexwolf) | 2022-09-05 |
✅ Make schema tests safer | [30](https://github.com/laminlabs/lndb-setup/pull/30) | [falexwolf](https://github.com/falexwolf) | 2022-08-30 |
🐛 Fix bugs related to setting up storage | [29](https://github.com/laminlabs/lndb-setup/pull/29) | [falexwolf](https://github.com/falexwolf) | 2022-08-30 |
🆙 Updated dependencies | [28](https://github.com/laminlabs/lndb-setup/pull/28) | [sunnyosun](https://github.com/sunnyosun) | 2022-08-26 | 0.6.1
✨ Make tables in schema modules configurable | [27](https://github.com/laminlabs/lndb-setup/pull/27) | [sunnyosun](https://github.com/sunnyosun) | 2022-08-25 | 0.6.0
🚸 Safer logging and errors for out-of-date schema | [26](https://github.com/laminlabs/lndb-setup/pull/26) | [falexwolf](https://github.com/falexwolf) | 2022-08-24 |
🎨 Use id to reference storage | [24](https://github.com/laminlabs/lndb-setup/pull/24) | [fredericenard](https://github.com/fredericenard) | 2022-08-23 | 0.5.5
✨ Track cloud workspace location | [23](https://github.com/laminlabs/lndb-setup/pull/23) | [fredericenard](https://github.com/fredericenard) | 2022-08-20 | 0.5.4
🚚 Rename schema modules | [22](https://github.com/laminlabs/lndb-setup/pull/22) | [falexwolf](https://github.com/falexwolf) | 2022-08-19 | 0.5.3
🔧 Switch to dedicated lndb-setup test bucket | [21](https://github.com/laminlabs/lndb-setup/pull/21) | [falexwolf](https://github.com/falexwolf) | 2022-08-18 | 0.5.2
🔊 Fix logging error | [20](https://github.com/laminlabs/lndb-setup/pull/20) | [falexwolf](https://github.com/falexwolf) | 2022-08-18 |
✨ Integrate instance setup with lndb-bfx-pipeline | [19](https://github.com/laminlabs/lndb-setup/pull/19) | [bpenteado](https://github.com/bpenteado) | 2022-08-18 |
🔊 Correct login error message | [18](https://github.com/laminlabs/lndb-setup/pull/18) | [fredericenard](https://github.com/fredericenard) | 2022-08-17 |
🧱 Settings with absolute path | [17](https://github.com/laminlabs/lndb-setup/pull/17) | [fredericenard](https://github.com/fredericenard) | 2022-08-10 | 0.5.1
✨ Add schema version check back in | [16](https://github.com/laminlabs/lndb-setup/pull/16) | [falexwolf](https://github.com/falexwolf) | 2022-08-03 | 0.5.0
🚸 Safe login in case of partially deleted instance | [14](https://github.com/laminlabs/lndb-setup/pull/14) | [falexwolf](https://github.com/falexwolf) | 2022-08-02 | 0.4.3
🐛 Log user into instance db | [13](https://github.com/laminlabs/lndb-setup/pull/13) | [falexwolf](https://github.com/falexwolf) | 2022-08-02 | 0.4.2
🚸 Remove handle from signup | [12](https://github.com/laminlabs/lndb-setup/pull/12) | [falexwolf](https://github.com/falexwolf) | 2022-08-02 |
✅ Test edge case | [11](https://github.com/laminlabs/lndb-setup/pull/11) | [falexwolf](https://github.com/falexwolf) | 2022-08-01 | 0.4.1
📝 Renamed top-level API functions to match CLI names | [10](https://github.com/laminlabs/lndb-setup/pull/10) | [falexwolf](https://github.com/falexwolf) | 2022-08-01 | 0.4.0
✨ Require registering a `user_handle` and rename `instance_name` to `name` | [9](https://github.com/laminlabs/lndb-setup/pull/9) | [falexwolf](https://github.com/falexwolf) | 2022-08-01 |
🚚 Rename `user_email` to `email`, `user_secret` to `password`, and `user_id` to `id` | [8](https://github.com/laminlabs/lndb-setup/pull/8) | [falexwolf](https://github.com/falexwolf) | 2022-08-01 |
🩹 Import core schema at setup time | [commit](https://github.com/laminlabs/lndb-setup/commit/1d12339836a06cfee25991f3e4ba3c7c73620570) | [falexwolf](https://github.com/falexwolf) | 2022-07-31 | 0.3.1
⬆️ Upgrade to `lamindb-schema` 0.3.1 | [7](https://github.com/laminlabs/lndb-setup/pull/7) | [falexwolf](https://github.com/falexwolf) | 2022-07-31 | 0.3.1
🚚 Make storage public | [6](https://github.com/laminlabs/lndb-setup/pull/6) | [falexwolf](https://github.com/falexwolf) | 2022-07-26 | 0.3.0
🚸 Expose settings through one `settings` class | [5](https://github.com/laminlabs/lndb-setup/pull/5) | [falexwolf](https://github.com/falexwolf) | 2022-07-26 |
♻️ Modularize settings & setup across files, increase coverage, set up documentation | [4](https://github.com/laminlabs/lndb-setup/pull/4) | [falexwolf](https://github.com/falexwolf) | 2022-07-26 |
✨ Allow switching schema modules | [3](https://github.com/laminlabs/lndb-setup/pull/3) | [falexwolf](https://github.com/falexwolf) | 2022-07-25 | 0.2.0
🚚 Add table construction & database engine | [2](https://github.com/laminlabs/lndb-setup/pull/2) | [falexwolf](https://github.com/falexwolf) | 2022-07-25 | 0.1.1
🚚 Move code from lamindb here | [1](https://github.com/laminlabs/lndb-setup/pull/1) | [falexwolf](https://github.com/falexwolf) | 2022-07-24 | 0.1.0
