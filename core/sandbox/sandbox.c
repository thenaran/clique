/* Copyright 2013 Narantech Inc. All Rights Reserved.
 *  __    _ _______ ______   _______ __    _ _______ _______ _______ __   __
 * |  |  | |   _   |    _ | |   _   |  |  | |       |       |       |  | |  |
 * |   |_| |  |_|  |   | || |  |_|  |   |_| |_     _|    ___|       |  |_|  |
 * |       |       |   |_||_|       |       | |   | |   |___|       |       |
 * |  _    |       |    __  |       |  _    | |   | |    ___|      _|       |
 * | | |   |   _   |   |  | |   _   | | |   | |   | |   |___|     |_|   _   |
 * |_|  |__|__| |__|___|  |_|__| |__|_|  |__| |___| |_______|_______|__| |__|
 */

/* This executable must be owned by the root user.
 *
 * Set-up:
 *  chown root:root sandbox; chmod 4511 sandbox
 *
 * Usage:
 *  sandbox <appname> <executable>
 *
 *  <executable> must exist in the /app directory.
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/capability.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <pwd.h>

#include "privdrop.h"


#define NUM_REQUIRED_ARGS 3
/* The entry file path buffer. It starts with "/app". */
#define BUFFER_SIZE 516
/* We assume the app user inside the chroot will always have uid/gid of 1913 */
#define APPUSER_UID 1913
#define APPUSER_GID 1913


int main(int argc, char *const argv[], char *const envp[])
{
  struct passwd *app_user;
  int ret = -1;
  int no_newpid_ns = 1;
  cap_value_t cap_list[4];
  char *appname;
  char *executable;
  char buffer[BUFFER_SIZE];

  if (argc < NUM_REQUIRED_ARGS) {
    fprintf(stderr, "Application name and main entry file must be specified\n");
    return EXIT_FAILURE;
  }

  appname = argv[1];
  executable = argv[2];

  /* In theory we do not rely on euid 0 -> euid N transition magic:
   * - we drop capabilities and we become non dumpable manually
   * - however we have not reviewed the sandbox in a capability-FS environment
   *   yet
   */
  if (geteuid()) {
    fprintf(stderr, "The sandbox is not seteuid root, aborting\n");
    return EXIT_FAILURE;
  }

  if (!getuid()) {
    fprintf(stderr,
        "The sandbox is not designed to be run by root, aborting\n");
    return EXIT_FAILURE;
  }

  /* capabilities we need */
  cap_list[0] = CAP_SETUID;
  cap_list[1] = CAP_SETGID;
  cap_list[2] = CAP_SYS_ADMIN;  /* for CLONE_NEWPID */
  cap_list[3] = CAP_SYS_CHROOT;

  /* Reduce capabilities to what we need. This is generally useless because:
   * 1. we will still have root euid (unless capability FS is used)
   * 2. the capabilities we keep are root equivalent
   * It's useful to drop CAP_SYS_RESSOURCE so that RLIMIT_NOFILE becomes
   * effective though
   */
  if (set_capabilities(cap_list, sizeof(cap_list)/sizeof(cap_list[0]))) {
    fprintf(stderr, "Could not adjust capabilities, aborting\n");
    return EXIT_FAILURE;
  }

  /* VERY IMPORTANT: CLONE_NEWPID and CLONE_FS should be in that order!
   * You can't share FS accross namespaces
   */

  /* Get a new PID namespace */
  no_newpid_ns=do_newpidns();
  if (no_newpid_ns) {
    fprintf(stderr, "Could not get new PID namespace\n");
    return EXIT_FAILURE;
  }

  snprintf(buffer, BUFFER_SIZE, "/apps/%s", appname);
  /* launch chroot helper */
  ret = do_chroot(buffer);
  if (ret) {
    fprintf(stderr, "Could not launch chroot helper\n");
    return EXIT_FAILURE;
  }

  /* could not switch uid */
  ret = do_setuid(APPUSER_UID, APPUSER_GID, appname);
  if (ret) {
    fprintf(stderr, "Could not properly drop privileges\n");
    return EXIT_FAILURE;
  }

  /* sanity check
   * FIXME: add capabilities
   */
  if (geteuid() == 0 || getegid() == 0 || !setuid(0) || !setgid(0)) {
    fprintf(stderr, "My euid or egid is 0! Something went really wrong\n");
    return EXIT_FAILURE;
  }

#ifdef DEBUG
  /* we are now unprivileged! */
  fprintf(stderr,
          "Hi from the sandbox! I'm pid=%ld, uid=%d, gid=%d, dumpable=%c\n",
          syscall(SYS_getpid), getuid(), getgid(), getdumpable()? 'Y' : 'N');
#endif
  snprintf(buffer, BUFFER_SIZE, "/app/%s", executable);
#ifdef DEBUG
  fprintf(stderr, "Executing %s: %s\n", appname, buffer);
#endif
  /* TODO: make the argv safer. */
  execve(buffer, argv + NUM_REQUIRED_ARGS - 1, envp);
  perror("exec failed");
  return EXIT_FAILURE;
}
