/* Copyright 2013 Narantech Inc. All Rights Reserved.
 */

#define _GNU_SOURCE
#include <sys/time.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <sys/prctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/capability.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <sched.h>

#include "privdrop.h"

#define NTHREADS 1024
#define CONFIRM_MSG 'C'

/* Create a helper process that will chroot the sandboxed process when required.
 */
int do_chroot(char *dir)
{
  int sv[2];
  int ret;
  ssize_t cnt;
  register pid_t pid;
  char msg;
  struct stat sdir_stat;

  if (stat(dir, &sdir_stat) || !S_ISDIR(sdir_stat.st_mode)) {
    fprintf(stderr, "do_chroot: %s does not exist ?!\n", dir);
    return -1;
  }

  ret = socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
  if (ret == -1) {
    perror("socketpair");
    return -1;
  }

  pid = syscall(SYS_clone, CLONE_FS | SIGCHLD, 0, 0, 0);

  switch (pid) {
  case -1:
    perror("clone");
    return -1;

  /* child */
  case 0:
    ret = chroot(dir);
    if (ret) {
      perror("Helper: chroot");
      exit(EXIT_FAILURE);
    }

    ret = chdir("/");
    if (ret) {
      perror("Helper: chdir");
      exit(EXIT_FAILURE);
    }

    msg = CONFIRM_MSG;
    /* Notify the parent */
    cnt = write(sv[0], &msg, 1);
    if (cnt != 1) {
      fprintf(stderr, "Helper: couldn't write confirmation\n");
      exit(EXIT_FAILURE);
    }
    exit(EXIT_SUCCESS);

  default:
    cnt = read(sv[1], &msg, 1);
    if (cnt != 1) {
      perror("Helper: read");
    }
    if (msg != CONFIRM_MSG) {
      fprintf(stderr, "Helper: invalid confirmation message\n");
      exit(EXIT_FAILURE);
    }

    ret = close(sv[0]);
    if (ret) {
      perror("Helper: close 0");
    }
    ret = close(sv[1]);
    if (ret) {
      perror("Helper: close 1");
    }
    return 0;
  }
}

/* return -1 as a failure */
int do_setuid(uid_t uid, gid_t gid, char *appname)
{
  /* We become explicitely non dumpable. Note that normally setuid() takes care
   * of this when we switch euid, but we want to support capability FS.
   */

  if ( setdumpable() || initgroups(appname, gid) ||
      setresgid(gid, gid, gid)||
      setresuid(uid, uid, uid)) {
    return -1;
  }

  /* Drop all capabilities. Again, setuid() normally takes care of this if we had
   * euid 0
   */
  if (set_capabilities(NULL, 0)) {
    return -1;
  }

  return 0;
}

/* we want to unshare(), but CLONE_NEWPID is not supported in unshare
 * so we use clone() instead, but we wait() for our child.
 */
int do_newpidns(void)
{
  register pid_t pid, waited;
  int status;

  pid = syscall(SYS_clone, CLONE_NEWPID | SIGCHLD, 0, 0, 0);

  switch (pid) {

  case -1:
    perror("clone");
    return -1;

  /* child: we are pid number 1 in the new namespace */
  case 0:
    /* We add an extra check for old kernels because sys_clone() doesn't
       EINVAL on unknown flags */
    /* getpid() has some caching issues so we must get the value by directly
       invoking the system call. */
    if (syscall(SYS_getpid) == 1)
      return 0;
    else
      return -1;

  default:
    /* Let's wait for our child */
    waited = waitpid(pid, &status, 0);
    if (waited != pid) {
      perror("waitpid");
      exit(EXIT_FAILURE);
    } else {
      /* FIXME: we proxy the exit code, but only if the child terminated normally */
      if (WIFEXITED(status))
        exit(WEXITSTATUS(status));
      exit(EXIT_SUCCESS);
    }
  }
}


/* set capabilities in all three sets
 * we support NULL / 0 argument to drop all capabilities
 */
int set_capabilities(cap_value_t cap_list[], int ncap)
{
  cap_t caps;
  int ret;

  /* caps should be initialized with all flags cleared... */
  caps=cap_init();
  if (!caps) {
    perror("cap_init");
    return -1;
  }
  /* ... but we better rely on cap_clear */
  if (cap_clear(caps)) {
    perror("cap_clear");
    return -1;
  }

  if ((cap_list) && ncap) {
    if (cap_set_flag(caps, CAP_EFFECTIVE, ncap, cap_list, CAP_SET) ||
        cap_set_flag(caps, CAP_INHERITABLE, ncap, cap_list, CAP_SET) ||
        cap_set_flag(caps, CAP_PERMITTED, ncap, cap_list, CAP_SET)) {
      perror("cap_set_flag");
      cap_free(caps);
      return -1;
    }
  }

  ret=cap_set_proc(caps);

  if (ret) {
    perror("cap_set_proc");
    cap_free(caps);
    return -1;
  }

  if (cap_free(caps)) {
    perror("cap_free");
    return -1;
  }

  return 0;
}

int getdumpable(void)
{
  int ret;

  ret = prctl(PR_GET_DUMPABLE, NULL, NULL, NULL, NULL);
  if (ret == -1) {
    perror("PR_GET_DUMPABLE");
  }
  return ret;
}

int setdumpable(void)
{
  int ret;

  ret = prctl(PR_SET_DUMPABLE, 0, NULL, NULL, NULL);
  if (ret == -1) {
    perror("PR_SET_DUMPABLE");
  }
  return ret;
}
