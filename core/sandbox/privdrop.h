/* Copyright 2009 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Julien Tinnes
 */
#ifndef PRIVDROP_H
#define PRIVDROP_H

#ifndef CLONE_NEWPID
#define CLONE_NEWPID  0x20000000
#endif

int do_chroot(char *dir);
int do_setuid(uid_t uid, gid_t gid, char *appname);
int do_newpidns(void);
int getdumpable(void);
int setdumpable(void);
int set_capabilities(cap_value_t cap_list[], int ncap);

#endif
