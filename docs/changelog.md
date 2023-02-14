# Changelog

<!-- prettier-ignore -->
Name | PR | Developer | Date | Version
--- | --- | --- | --- | ---
🚑 Generate all migrate files before migrate | [274](https://github.com/laminlabs/lndb/pull/274) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.2
🚑 Fix import and simplify params | [273](https://github.com/laminlabs/lndb/pull/273) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-14 | 0.32.1
⬆️ Upgrade `lnhub_rest` to 0.3.2 | [272](https://github.com/laminlabs/lndb/pull/272) | [bpenteado](https://github.com/bpenteado) | 2023-02-13 |
🚚 Rename package from `lndb_setup` to `lndb` | [270](https://github.com/laminlabs/lndb/pull/270) | [falexwolf](https://github.com/falexwolf) | 2023-02-12 | 0.32.0
🎨 Enable using the current instance for generating migration script | [271](https://github.com/laminlabs/lndb-setup/pull/271) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 | 0.31.0
🎨 Generate alembic.ini before before check migrate | [269](https://github.com/laminlabs/lndb-setup/pull/269) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 |
✨ Added migration module | [266](https://github.com/laminlabs/lndb-setup/pull/266) | [sunnyosun](https://github.com/sunnyosun) | 2023-02-10 |
🐛 Fix access to newly created instances on s3 due to region | [268](https://github.com/laminlabs/lndb-setup/pull/268) | [Koncopd](https://github.com/Koncopd) | 2023-02-09 |
🚸 Validate instances at `init` | [264](https://github.com/laminlabs/lndb-setup/pull/264) | [falexwolf](https://github.com/falexwolf) | 2023-02-09 |
🚸 Add error if client version schema package version is lower than deployed db schema module version | [267](https://github.com/laminlabs/lndb-setup/pull/267) | [falexwolf](https://github.com/falexwolf) | 2023-02-09 |
:recycle: Remove _sbclient suffix | [263](https://github.com/laminlabs/lndb-setup/pull/263) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 |
🚸 Better UX `delete()` | [260](https://github.com/laminlabs/lndb-setup/pull/260) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 |
🐛 Fix check_versions | [262](https://github.com/laminlabs/lndb-setup/pull/262) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 | 0.30.14
🚑 Fix login on new environments | [261](https://github.com/laminlabs/lndb-setup/pull/261) | [falexwolf](https://github.com/falexwolf) | 2023-02-06 | 0.30.13
🐛 Fix modified for gcs for locker | [258](https://github.com/laminlabs/lndb-setup/pull/258) | [Koncopd](https://github.com/Koncopd) | 2023-02-05 |
✨ Add rich string representation for InstanceSettings class | [254](https://github.com/laminlabs/lndb-setup/pull/254) | [bpenteado](https://github.com/bpenteado) | 2023-01-31 | 0.30.12
✨ Add rich string representation for UserSettings class | [255](https://github.com/laminlabs/lndb-setup/pull/255) | [bpenteado](https://github.com/bpenteado) | 2023-01-31 |
📌 Pin deps for aiobotocore to fix resolution | [commit](https://github.com/laminlabs/lndb-setup/commit/439898124d09f7cab323a1ef275e72eba072a4d7) | [Koncopd](https://github.com/Koncopd) | 2023-01-27 | 0.30.11
⬆️ Upgrade lnhub-rest to 0.1.4 | [252](https://github.com/laminlabs/lndb-setup/pull/252) | [fredericenard](https://github.com/fredericenard) | 2023-01-27 |
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
⬆️ Upgrade lnhub-rest | [225](https://github.com/laminlabs/lndb-setup/pull/225) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.2
🔥 Remove atexit | [224](https://github.com/laminlabs/lndb-setup/pull/224) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 |
⬆️ Upgrade lnhub_rest | [223](https://github.com/laminlabs/lndb-setup/pull/223) | [falexwolf](https://github.com/falexwolf) | 2023-01-16 | 0.30.1
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
✨ Output information about current user in `lndb info` | [185](https://github.com/laminlabs/lndb-setup/pull/185) | [falexwolf](https://github.com/falexwolf) | 2023-01-05 |
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
🔥 Remove lndb hub depency | [86](https://github.com/laminlabs/lndb-setup/pull/86) | [fredericenard](https://github.com/fredericenard) | 2022-10-26 | 0.13.0
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
