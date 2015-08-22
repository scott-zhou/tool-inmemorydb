/**
  * Use printf to do function test and debug maybe okay, but it's not a good
  * idea in a serious product.
  * Here I defind inmdb_log MACRO and use printf to output logs.
  * If you want to take my library in your product, use you own log finction
  * to replace my MACRO.
  * I recommend my another library tool-log for asynchronous log/trace:
  * https://github.com/scott-zhou/tool-log
  */

#ifndef __INMDB_LOG_H__
#define __INMDB_LOG_H__

#define	LOGDEBUG	5
#define LOGWARN		4
#define LOGMAJOR	3
#define LOGMINOR	2
#define LOGCRITICAL	1

#define inmdb_log(level, format,...) printf("%s:%d:%s LEVEL %d: "format"\n",__FILE__,__LINE__,__FUNCTION__,level,__VA_ARGS__)

#endif
