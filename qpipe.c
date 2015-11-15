/*   remote pipe from a server to another server */
/*   TangCheng Write 2011.07                     */

/* gcc -Wunused -g -lpthread -lz qpipe.c -o qpipe */
/* gcc -Wunused -g -D_REENTRANT -D_POSIX_PTHREAD_SEMANTICS -lsocket  -lnsl -lpthread -lz -lrt qpipe.c -o qpipe */


#undef   _FILE_OFFSET_BITS
#define  _FILE_OFFSET_BITS  64
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <pthread.h>
#include <zlib.h>
#include <semaphore.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <stdint.h>

#include <stdarg.h>
#include <limits.h>

/*服务端最多可以同时并发运行的JOB*/
#define MAX_PARALLEL_JOB   1024

#define CMD_MAX_LENGTH    4096

#define NET_RETRY_CNT       10
#define NET_TIMEOUT_SECOND  30

#define  NEW_CONNECT_TAG  0x0
#define  RE_CONNECT_TAG   0x01

#define  SEQ_PACKET_TAG   0x02
#define  END_PACKET_TAG   0xFFFFFFFF

#define  HEART_BEAT_PACKET_TAG 0x0F
#define  HEART_BEAT_INTERVAL   10



#define SYS_ERRMSG_MAX_SIZE 512

#define TCP_NODELAY    0x01

#define ERR_LOG    0x01
#define INFO_LOG   0x02
#define STAT_LOG   0x04
#define DEBUG0_LOG 0x08
#define DEBUG1_LOG 0x10

#define MAX_NUMS_OF_ALLOW_NETWORK 256

int g_sock_buf_size = 1048576;


void signal_func(int no);
void set_signal();
int g_iIsAbort=0;

int g_loglevel = 3;

int g_serv_wait_reconnect_time = 600;


/*这用于client端，标识每个client端的一个唯一编号*/
uint32_t g_clientid = 0;

typedef struct
{
    char servip[256];
    int port;
    int threadnums;
    int BlkSz;
    int zlevel; /*compression level*/
    char szCmd[2048];

    in_addr_t netip[MAX_NUMS_OF_ALLOW_NETWORK];
    int  masklen[MAX_NUMS_OF_ALLOW_NETWORK];
    int netcnt;
}PROG_ARGS;

typedef struct
{
    int BlkCnt;
    int BlkSz;
    char * srcbuf;
    char * dstbuf;
    int * srcbuflen;
    int * dstbuflen;

    sem_t * sem_srcbuf_isempty;
    sem_t * sem_srcbuf_havedata;
    sem_t * sem_dstbuf_isempty;
    sem_t * sem_dstbuf_havedata;
}QUICKBUFFER;

typedef struct
{
    double st;
    double allt;
    int cnt;
    time_t start_time;
}MEANTIMESTAT;

typedef struct
{
    double st;
    int64_t value;
    pthread_mutex_t mutex;
    pthread_mutexattr_t attr;
}RATESTAT;


typedef struct
{
    pthread_attr_t tattr;
    pthread_t tid;
    int fd;
    const char * servip;
    int port;
    QUICKBUFFER * pQB;
    uint64_t seq;
    int * pIsAbort;
    int *pCompressIsOver;

    MEANTIMESTAT ts_send;
    MEANTIMESTAT ts_respond;

    RATESTAT * pReadRateStat;
    RATESTAT * pSendRateStat;
    int64_t  * pSendBytes;
}SENDTHREADPARA;

typedef struct
{
    pthread_attr_t tattr;
    pthread_t tid;
    int id;
    QUICKBUFFER * pQB;
    int * pIsAbort;
    int * pReadIsOver;
    int zlevel; /*compression level*/
}COMPRESSTHREADPARA;

typedef struct
{
    pthread_attr_t tattr;
    pthread_t tid;
    uint32_t clientid; /*连接过来的客户端id*/
    int id;
    QUICKBUFFER * pQB;
    int * pReadIsOver;
    int * pIsAbort;
}UNCOMPRESSTHREADPARA;


typedef struct
{
    int isused; /*是否正在使用*/
    pthread_attr_t tattr;
    pthread_t tid;
    int fd; /*socket fd*/
    int refd; /*reconnect socked fd*/
    pthread_mutex_t m_mutex;

    UNCOMPRESSTHREADPARA * UnCompressThreadData;
    int threadnums;
    int BlkSz;
    uint32_t clientid; /*连接过来的客户端id*/
    char * szCmd;
    QUICKBUFFER * pQB;
    uint64_t PacketSeq;
}SERVERJOBTHREADDATA;

uint32_t g_clientid_seq = 0;
SERVERJOBTHREADDATA * g_serv_job_data;
pthread_mutex_t g_serv_job_data_mutex;

typedef struct
{
    pthread_attr_t tattr;
    pthread_t tid;
    uint32_t clientid; /*连接过来的客户端id*/
    QUICKBUFFER * pQB;
    FILE * fp; /*popen return fp*/
    int * pUnCompressIsOver;
    int * pIsAbort;
}WRITETHREADPARA;


void PrtLog(int iLogLevel,const char * format,...);

off_t GetArgValue(char * szValue);

void MeanTimeStatStart(MEANTIMESTAT * pStat);
void MeanTimeStatEnd(MEANTIMESTAT * pStat, int cnt, const char * msg);

static double time_so_far();

int GetBinPath(char * argv0,char * szPath);
int CreatePidFile(char * pidfile);
int CheckIsRunning(char * pidfile);

uint64_t GetI64Msg(char *buf);
void SetI64Msg(char *buf, uint64_t value);

RATESTAT * AllocRateStat();
void FreeRateStat(RATESTAT * pStat);
void RateStatAddValue(RATESTAT * pStat, int64_t value);
void GetRateStat(RATESTAT * pStat, double * pSec, int64_t * pValue);
void GetRateStatAndReset(RATESTAT * pStat, double * pSec, int64_t * pValue);
void RateStatReset(RATESTAT * pStat);


int ReadData(int fd, char * p, int len);

int sem_wait_abort(sem_t * se, int * pIsAbort);
int sem_wait_cond(sem_t * se, int * pIsOver, int * pIsAbort);

void * CompressThreadProc(void * pPara);
void * SendDataThreadProc(void * pPara);
int RunClient(PROG_ARGS * qpipe_args);


void * ServerJobThreadProc(void * pPara);
int g_listensock = -1;
void * WriteDataThreadProc(void * pPara);
void * UnCompressThreadProc(void * pPara);
int RunServer(PROG_ARGS * qpipe_args);

void set_nonblock(int socket);
int CloseSocketImmediate(int sockfd);
int OptimizedSocket(int sockfd, int sock_buf_size);
int SocketRead(int sockfd,char * p,int len);
int SocketWrite(int sockfd,char * p,int len);
int ReadPacket(int sockfd,char * p);
int SendPacket(int sockfd,char * p, int len);

/* 测试socket是否有数据到达了，返回0表示超进，返回1表示可以读数据了，返回-1表示网络错误 */
int TestCanRecv(int fd,int timeoutms,char * szErrInfo);
/*测试socket是否能发送数据，返回0表示超时，返回1表示可以发送数据了，返回-1表示网络错误 */
int TestCanSend(int sockfd, int timeoutms, char * szErrInfo);

void GetSysErrorInfo(int err_no,char * szErrInfo);


int CompressData(char * inbuf,int inlen,char * outbuf,int * poutlen);
int UnCompressData(char * inbuf,int inlen,char * outbuf,int * poutlen);

int usage()
{
    const char * usageinfo=
"Usage:   qpipe [OPTION]\n" \
"read data from stdin, and send to a command in remove server.\n\n" \
"  -s <server_ip>\n"\
"       in client mode, specify the server IP\n"\
"  -p <port>\n"\
"       in client mode, specify the server ip port,\n"\
"       in server mode, specify the server listen port\n"\
"  -c <cmd>\n"\
"       only can be used in client mode, specify the command run in server\n"\
"  -z <0-9>\n"\
"       specified compression level, between 0 and 9: 1 gives best speed, 9 gives best compression, 0 gives no compression at all\n"\
"  -t <1-nn>\n"\
"       specified number of concurrent compress threads, default is 4\n"\
"  -b <blocksize>\n"
"       specified block size, default is 4M\n"\
"  -l <loglevel>\n"\
"       loglevel, default loglevel is 3, 3 is 1(ERR_LOG)+2(INFO_LOG)\n"\
"                 ERR_LOG: 1, INFO_LOG: 2, STAT_LOG: 4, DEBUG0_LOG: 8, DEBUG1_LOG: 16\n"\
"  -w <wait_seconds>\n"\
"       specified time of server wait client reconnect, default is 300 seconds\n"\
"       when beyond it ,then server close this session\n"\
"  -n <allow_ip_or_network>\n"\
"       only can be used in server mode, specify the  specified client ipaddress or network can connect to server\n"\
"  -k <socket_buffer_size>\n"\
"       specified size of socket buffer,default is 1M\n"\
"  -h\n"\
"       help\n"\
"Example:\n"\
"   qpipe -p 5000 -n 172.16.196.0/24\n"\
"       This command start server in 5000 port.\n"\
"       If you only specify a port option, program run in server mode, and listen on this port.\n"\
"   cat t.dat |qpipe -s 192.168.1.200 -p 5000 -c \"cat >t2.dat\"\n"\
"       This command send file t.dat to 192.168.1.200\n";
    printf("%s\n", usageinfo);
    return 0;
}

QUICKBUFFER * AllocQuickBuffer(int BlkCnt, int BlkSz)
{
    QUICKBUFFER * pQB = (QUICKBUFFER*) malloc(sizeof(QUICKBUFFER));
    int i;

    pQB->BlkCnt = BlkCnt;
    pQB->BlkSz = BlkSz;
    pQB->sem_srcbuf_isempty   = (sem_t *)malloc(2*pQB->BlkCnt*sizeof(sem_t));
    pQB->sem_srcbuf_havedata  = (sem_t *)malloc(2*pQB->BlkCnt*sizeof(sem_t));
    pQB->sem_dstbuf_isempty  = (sem_t *)malloc(2*pQB->BlkCnt*sizeof(sem_t));
    pQB->sem_dstbuf_havedata = (sem_t *)malloc(2*pQB->BlkCnt*sizeof(sem_t));

    for(i=0; i<pQB->BlkCnt; i++)
    {
        sem_init( &pQB->sem_srcbuf_isempty[i], 0, 1);
        sem_init( &pQB->sem_srcbuf_isempty[pQB->BlkCnt+i], 0, 1);
        sem_init( &pQB->sem_srcbuf_havedata[i],0, 0);
        sem_init( &pQB->sem_srcbuf_havedata[pQB->BlkCnt+i], 0, 0);

        sem_init( &pQB->sem_dstbuf_isempty[i],0, 1);
        sem_init( &pQB->sem_dstbuf_isempty[pQB->BlkCnt+i], 0, 1);
        sem_init( &pQB->sem_dstbuf_havedata[i],0, 0);
        sem_init( &pQB->sem_dstbuf_havedata[pQB->BlkCnt+i], 0, 0);
    }

    pQB->srcbuf    = malloc(pQB->BlkCnt*(pQB->BlkSz+1)*2);
    pQB->dstbuf    = malloc(pQB->BlkCnt*(pQB->BlkSz+1)*2);
    pQB->srcbuflen = malloc(pQB->BlkCnt*sizeof(int)*2);
    pQB->dstbuflen = malloc(pQB->BlkCnt*sizeof(int)*2);
    return pQB;
}

int FreeQuickBuffer(QUICKBUFFER * pQB)
{
    int i;

    for(i=0; i<pQB->BlkCnt; i++)
    {
        sem_destroy( &pQB->sem_srcbuf_isempty[i]);
        sem_destroy( &pQB->sem_srcbuf_isempty[pQB->BlkCnt+i]);
        sem_destroy( &pQB->sem_srcbuf_havedata[i]);
        sem_destroy( &pQB->sem_srcbuf_havedata[pQB->BlkCnt+i]);

        sem_destroy( &pQB->sem_dstbuf_isempty[i]);
        sem_destroy( &pQB->sem_dstbuf_isempty[pQB->BlkCnt+i]);
        sem_destroy( &pQB->sem_dstbuf_havedata[i]);
        sem_destroy( &pQB->sem_dstbuf_havedata[pQB->BlkCnt+i]);
    }

    free(pQB->sem_srcbuf_isempty);
    free(pQB->sem_srcbuf_havedata);
    free(pQB->sem_dstbuf_isempty);
    free(pQB->sem_dstbuf_havedata);

    free(pQB->srcbuf);
    free(pQB->dstbuf);
    free(pQB->srcbuflen);
    free(pQB->dstbuflen);
    free(pQB);
    return 0;
}

unsigned int GetMaskByLen(int len)
{
    unsigned int value;
    unsigned int n;

    value=0xFFFFFFFF;
    if ( len >= 32)
    {
        return value;
    }
    if ( len == 0)
    {
        return 0;
    }

    n = 32-len;
    value = value - ( (1<<n) -1);
    return value;
}

int AddAllowNetwork(PROG_ARGS * pArgs, char * optarg)
{
    struct sockaddr_in sin;
    char szOptArg[32];
    char * p;
    char * szNetIp;
    int len;
    int masklen;

    if (pArgs->netcnt >=MAX_NUMS_OF_ALLOW_NETWORK)
    {
        return -2;
    }


    len = strlen(optarg);
    if (len > 18)
    {
        return -1;
    }
    strcpy(szOptArg, optarg);

    p = strstr(szOptArg, "/");
    if (p == NULL)
    {
        masklen = 32;
        szNetIp =  szOptArg;
    }
    else
    {
        masklen = atoi(&p[1]);
        if (masklen < 0)
        {
            return -1;
        }
        p[0]='\0';
        szNetIp = szOptArg;
    }

    if ((sin.sin_addr.s_addr = inet_addr(szNetIp)) == INADDR_NONE)
    {
        return -1;
    }

    pArgs->netip[pArgs->netcnt] = sin.sin_addr.s_addr;
    pArgs->masklen[pArgs->netcnt] = masklen;
    pArgs->netcnt++;
    return 0;
}

int main(int argc, char * argv[])
{
    int ret;
    int ch;
    PROG_ARGS qpipe_args;
    int bIsClient = 0;

    memset(&qpipe_args, 0, sizeof(PROG_ARGS));
    qpipe_args.BlkSz = 1024*1024*4;
    qpipe_args.threadnums = 4;
    qpipe_args.zlevel = 1;
    qpipe_args.netcnt = 0;


    opterr=0;
    while ( (ch = getopt(argc,argv,"hl:s:p:c:z:t:b:w:n:k:")) != -1)
    {
        switch (ch)
        {
        case 'h':
            usage(argv[0]);
            return 0;
        case 's':
            strcpy(qpipe_args.servip, optarg);
            bIsClient = 1;
            break;
        case 'p':
            qpipe_args.port = atoi(optarg);
            break;

        case 'c':
            strcpy(qpipe_args.szCmd, optarg);
            break;

        case 'z':
            qpipe_args.zlevel = atoi(optarg);
            break;

        case 't':
            qpipe_args.threadnums = atoi(optarg);
            break;

        case 'b':
            qpipe_args.BlkSz = GetArgValue(optarg);
            break;
        case 'k':
            g_sock_buf_size  = GetArgValue(optarg);
            break;

        case 'l':
            g_loglevel = atoi(optarg);
            break;
        case 'w':
            g_serv_wait_reconnect_time = atoi(optarg);
            break;

        case 'n':
            ret = AddAllowNetwork(&qpipe_args, optarg);
            if (ret == -1)
            {
                printf("Invalid -n parameter : %s\n", optarg);
                return 1;
            }
            else if (ret  == -2)
            {
                printf("Too many -n parameter, numbers of -n parameter can not great than %d\n", MAX_NUMS_OF_ALLOW_NETWORK);
                return 1;
            }
            break;

        default:
            usage(argv[0]);
            return 1;
        }
    }


    if (qpipe_args.port <= 0)
    {
        PrtLog(ERR_LOG,"invalid ip port %d or not specify ip port option( -p) !\n", qpipe_args.port);
        return 1;
    }

    if (g_serv_wait_reconnect_time <= NET_TIMEOUT_SECOND)
    {
        PrtLog(ERR_LOG,"the time that server wait client reconnect must more than %d!\n", NET_TIMEOUT_SECOND);
        return 1;
    }


    if(bIsClient)
    {
        if (qpipe_args.threadnums<=0)
        {
            PrtLog(ERR_LOG,"threadNums must more than zero!\n");
            return 1;
        }

        if (qpipe_args.BlkSz<=0)
        {
            PrtLog(ERR_LOG,"blocksize must more than zero!\n");
            return 1;
        }
        if (qpipe_args.szCmd[0] == '\0')
        {
            PrtLog(ERR_LOG,"Must specify -c option in client mode!\n");
            return 1;
        }

        if (inet_addr(qpipe_args.servip) == INADDR_NONE)
        {
            PrtLog(ERR_LOG,"invalid ip address %s!\n", qpipe_args.servip);
            return 1;
        }

        if (qpipe_args.zlevel < 0 || qpipe_args.zlevel > 9)
        {
            PrtLog(ERR_LOG,"compression level must between 0 and 9, can not be %d!\n", qpipe_args.zlevel);
        }
        if (qpipe_args.zlevel  == 0) //不压缩
        {
            qpipe_args.threadnums = 1;
        }
        ret = RunClient(&qpipe_args);
    }
    else
    {
        if (qpipe_args.netcnt<=0)
        {
            PrtLog(ERR_LOG,"Must specify -n parameter!\n");
            return 1;
        }
        char szPidFile[PATH_MAX];
        GetBinPath(argv[0], szPidFile);
        strcat(szPidFile,"/qpipe.pid");

        if (CheckIsRunning(szPidFile))
        {
            PrtLog(ERR_LOG, "Program already running!!!\n");
            return -1;
        }

        if (CreatePidFile(szPidFile) != 0)
        {
            return -1;
        }

        ret = RunServer(&qpipe_args);
        unlink(szPidFile);
    }
    return ret;
}

int ConnectToServer(const char * szDestIp, int iPort, int ms)
{
    struct sockaddr_in sin;
    int fd;
    int iRet;
    struct timeval timeo;
    //fd_set set;

    timeo.tv_sec = ms /1000;
    timeo.tv_usec = (ms %1000)*1000;

    memset(&sin,0,sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(iPort);

    if ((sin.sin_addr.s_addr = inet_addr(szDestIp)) == INADDR_NONE)
    {
        PrtLog(ERR_LOG, "ip address %s is invalid!\n",szDestIp);
        return -1;
    }

    fd = socket(PF_INET,SOCK_STREAM,0);
    if (fd < 0)
    {
        PrtLog(ERR_LOG, "Create socket error");
        return -1;
    }

    //fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

    iRet = connect(fd, (struct sockaddr *)&sin, sizeof(sin));
    if (iRet == 0)
    {
        goto success_exit;
    }
    if (iRet < 0)
    {
        //if (errno != EINPROGRESS)
        //{
            PrtLog(ERR_LOG, "Can't connect to %s:%d\n", szDestIp, iPort);
            close(fd);
            return -1;
        //}
    }

    /*
    FD_ZERO(&set);
    FD_SET(fd, &set);
    iRet = select(fd + 1, NULL, &set, NULL, &timeo);
    if (iRet == -1)
    {
        PrtLog(ERR_LOG, "call select failed!");
        close(fd);
        return -1;
    }
    else if(iRet == 0)
    {

        close(fd);
        return 0;
    }

    int error=-1;
    socklen_t len;
    len = 4;
    iRet = getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&error, &len);
    if(error != 0)
    {
        PrtLog(ERR_LOG, "Can not connect to %s:%d\n",szDestIp, iPort);
        return -1;
    }
    */

success_exit:
    //fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & (~O_NONBLOCK) );

    OptimizedSocket(fd, g_sock_buf_size);
    //li.l_onoff = 1;
    //li.l_linger = 10;
    //setsockopt(fd,SOL_SOCKET, SO_LINGER, (char *) &li, sizeof(li));

    int flag = 1;
    iRet = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag) );

    return fd;
}

int GetIntMsg(char * buf)
{
   int iRet;
   memcpy(&iRet, buf, 4);
   iRet = ntohl(iRet);
   return iRet;
}

void SetIntMsg(char *buf, int value)
{
    value = htonl(value);
    memcpy(buf, &value, 4);
}

uint64_t GetI64Msg(char *buf)
{
    char * p;
    uint64_t value;
    int i;

    if (ntohl(1) == 1) /* host order is same as network */
	{
		memcpy((void *)&value, buf, 8);
	}
	else /* host order is not same as network */
	{
		p = (char *)&value;
        for (i=0; i<8; i++)
        {
            p[i] = buf[7 - i];
        }
	}
    return value;
}

void SetI64Msg(char *buf, uint64_t value)
{
    char * p;
    int i;

    if (ntohl(1) == 1) /* host order is same as network */
	{

		memcpy(buf, (void *)&value, 8);
	}
	else /* host order is not same as network */
	{
		p = (char *)&value;
        for (i=0; i<8; i++)
        {
            buf[i] = p[7 - i];
        }
	}
}


int RunClient(PROG_ARGS * pArgs)
{
    int i;
    int iBufFlag;
    int iLeftSize;
    int datalen;
    char buf[4192];
    int iRetCode = 0;
    int filefd = STDIN_FILENO;
    ssize_t nReadSize;
    int threadnums;
    int fd;
    int ReadIsOver = 0;
    int CompressIsOver = 0;
    int iRet;
    COMPRESSTHREADPARA * CompressThreadPara = NULL;
    QUICKBUFFER * pQB;
    RATESTAT * pReadRateStat;
    RATESTAT * pSendRateStat;
    int64_t  AllSendBytes = 0;
    int64_t  AllReadBytes = 0;
    double   starttime = time_so_far();
    double   endtime;


    threadnums = pArgs->threadnums;
    pQB =  AllocQuickBuffer(threadnums, pArgs->BlkSz);

    fd = ConnectToServer(pArgs->servip, pArgs->port, NET_TIMEOUT_SECOND*1000);
    if (fd == -1)
    {
        return -1;
    }

    if (fd == 0)
    {
        PrtLog(ERR_LOG, "connect time out(%d seconds)\n", NET_TIMEOUT_SECOND);
        return -1;
    }

    PrtLog(DEBUG0_LOG, "Connect Server %s:%d successfully.\n", pArgs->servip, pArgs->port);

    /*前4个字节，是命令类型: NEW_CONNECT_TAG*/
    SetIntMsg(buf, NEW_CONNECT_TAG);
    /*后面,4个字符表示线程数，4个字节BlkSz，4个字节表示socket buffer size ,再后面是命令*/
    SetIntMsg(&buf[4], pArgs->threadnums);
    SetIntMsg(&buf[8], pArgs->BlkSz);
    SetIntMsg(&buf[12], g_sock_buf_size);

    strcpy(&buf[16], pArgs->szCmd);
    datalen = 16+strlen(&buf[16]);
    iRet = SendPacket(fd,buf,datalen);

    /*接收对方的响应包*/
    iRet=ReadPacket(fd, buf);
    if(iRet<=0)
    {
        PrtLog(ERR_LOG, "read packet failed , maybe network error!\n");
        close(fd);
        return -1;
    }
    buf[iRet] = 0;
    iRetCode = GetIntMsg(buf);

    if(iRetCode != 0)
    {
        PrtLog(ERR_LOG, "Server return error: %s\n",&buf[4]);
        close(fd);
        return -1;
    }

    /*server返给客户端一个clientid，连接中断后，重新连接的时候，
    客户端需要把这个clientid送给服务端，以标识自己*/

    g_clientid = GetIntMsg(&buf[4]);
    PrtLog(INFO_LOG, "Client id : %d\n", g_clientid);

    /*成功后，客户端直接发送文件的内容数据到服务器端*/

    set_signal();

    pReadRateStat = AllocRateStat();
    pSendRateStat = AllocRateStat();

    if (pArgs->zlevel >0) //压缩级别大于0时，才需要压缩
    {
        CompressThreadPara = (COMPRESSTHREADPARA *)malloc(sizeof(COMPRESSTHREADPARA)*pQB->BlkCnt);
        /*创建压缩数据的线程*/
        for(i=0; i<threadnums; i++)
        {
            CompressThreadPara[i].id = i;
            CompressThreadPara[i].pQB = pQB;
            CompressThreadPara[i].pIsAbort = &g_iIsAbort;
            CompressThreadPara[i].pReadIsOver = &ReadIsOver;
            CompressThreadPara[i].zlevel = pArgs->zlevel;

            pthread_attr_init(&CompressThreadPara[i].tattr);
            pthread_create (&CompressThreadPara[i].tid,
                            &CompressThreadPara[i].tattr,
                            CompressThreadProc,
                            (void *)&CompressThreadPara[i]);
        }
    }

    /*创建发送数据的线程*/
    SENDTHREADPARA SendThreadData;
    memset(&SendThreadData, 0, sizeof(SENDTHREADPARA));

    SendThreadData.pQB = pQB;
    SendThreadData.servip = pArgs->servip;
    SendThreadData.port = pArgs->port;
    SendThreadData.seq = 1;
    SendThreadData.fd = fd;
    SendThreadData.pIsAbort = &g_iIsAbort;
    SendThreadData.pReadRateStat = pReadRateStat;
    SendThreadData.pSendRateStat = pSendRateStat;
    SendThreadData.pSendBytes    = &AllSendBytes;

    SendThreadData.pCompressIsOver = &CompressIsOver;

    pthread_attr_init(&SendThreadData.tattr);
    pthread_create(&SendThreadData.tid,
                   &SendThreadData.tattr,
                   SendDataThreadProc,
                   (void *)&SendThreadData);

    PrtLog(DEBUG0_LOG, "Start to read data.\n");

    iBufFlag=0;
    do
    {
        for(i=0; i < threadnums; i++)
        {
            if (pArgs->zlevel >0)
            {
                iRet = sem_wait_abort(&pQB->sem_srcbuf_isempty[iBufFlag*pQB->BlkCnt+i], &g_iIsAbort);
            }
            else
            {
                iRet = sem_wait_abort(&pQB->sem_dstbuf_isempty[iBufFlag*pQB->BlkCnt+i], &g_iIsAbort);
            }

            if (iRet == -1)
            {
                break;
            }
        }
        if ( g_iIsAbort)
        {
            iRetCode = -1;
            break;
        }

        if (pArgs->zlevel >0)
        {

            nReadSize = ReadData(filefd,
                           &pQB->srcbuf[iBufFlag*pQB->BlkCnt*pQB->BlkSz],
                           pQB->BlkSz*pQB->BlkCnt);
        }
        else
        {
            nReadSize = ReadData(filefd,
                           &pQB->dstbuf[iBufFlag*pQB->BlkCnt*(pQB->BlkSz+1)+1],
                           pQB->BlkSz*pQB->BlkCnt);
            pQB->dstbuf[iBufFlag*pQB->BlkCnt*(pQB->BlkSz+1)]='\0';
        }

        PrtLog(DEBUG1_LOG, "Read data length = %d\n", (int)nReadSize);

        if(nReadSize < 0)
        {
             g_iIsAbort = 1;
             PrtLog(ERR_LOG, "Read file error, all thread will exit.\n");
             nReadSize=0;
        }

        AllReadBytes += nReadSize;
        RateStatAddValue(pReadRateStat, nReadSize);

        if (nReadSize > 0) /*还没有结束*/
        {
            if (pArgs->zlevel >0)
            {
                iLeftSize = nReadSize;
                for(i=0; i<pQB->BlkCnt; i++)
                {
                    if(iLeftSize > pQB->BlkSz)
                    {
                        pQB->srcbuflen[iBufFlag*pQB->BlkCnt+i] = pQB->BlkSz;
                    }
                    else
                    {
                        pQB->srcbuflen[iBufFlag*pQB->BlkCnt+i] = iLeftSize;
                    }
                    iLeftSize -= pQB->BlkSz;
                    if(iLeftSize < 0)
                    {
                        iLeftSize = 0;
                    }
                }
            }
            else
            {
                pQB->dstbuflen[iBufFlag*pQB->BlkCnt] = nReadSize;
            }
        }
        else  /*读结束了*/
        {
            if (pArgs->zlevel >0)
            {
                for(i=0; i<pQB->BlkCnt; i++)
                {
                    pQB->srcbuflen[iBufFlag*pQB->BlkCnt+i] = -1; /*长度为-1表示结束了，没有数据了*/
                }
            }
            else
            {
                pQB->dstbuflen[iBufFlag*pQB->BlkCnt] = -1;
            }
        }

        for(i=0; i<threadnums; i++)
        {
            if (pArgs->zlevel >0)
            {
                sem_post(&pQB->sem_srcbuf_havedata[iBufFlag*pQB->BlkCnt+i]);
            }
            else
            {
                sem_post(&pQB->sem_dstbuf_havedata[iBufFlag*pQB->BlkCnt+i]);
            }
        }
        iBufFlag =! iBufFlag;
    }while (nReadSize>0);

    PrtLog(DEBUG0_LOG, "Read data is over.\n");

    if (pArgs->zlevel > 0)
    {
        for(i=0; i<threadnums; i++)
        {
            pthread_join(CompressThreadPara[i].tid, NULL);
        }
    }

    pthread_join(SendThreadData.tid, NULL);

    close(fd);

    FreeRateStat(pReadRateStat);
    FreeRateStat(pSendRateStat);

    FreeQuickBuffer(pQB);

    if (pArgs->zlevel >0)
    {
        free(CompressThreadPara);
    }

    endtime = time_so_far();
    int compress_rate =0;
    if (AllReadBytes==0)
    {
        compress_rate=100;
    }
    else
    {
        compress_rate = (int)(AllSendBytes*100/AllReadBytes);
    }
    printf("====Total read bytes : %lld, send bytes : %lld, rate of compression : %d%%, time : %.2f s\n",
            (long long int)AllReadBytes,
            (long long int)AllSendBytes,
            compress_rate,
            endtime - starttime);

    return iRetCode;
}

/*返回0，表示满足条件，返回-1，表示发现IsAbort为真了，所以退出，返回1，表示，发现pIsOver为真了。*/
int sem_wait_cond(sem_t * se, int * pIsOver, int * pIsAbort)
{
    struct timespec tmout;
    int iRet;

    do
    {
        if (*pIsAbort)
        {
            return -1;
        }

        tmout.tv_sec = time(NULL)+1;
        tmout.tv_nsec = 0;

        if (*pIsOver)
        {
            iRet = sem_timedwait(se, &tmout);
            if (iRet < 0) /*超时，而pIsOver为1，则说明已没有数据了，可以结束了*/
            {
                return 1;
            }
        }
        else
        {
            iRet = sem_timedwait(se, &tmout);
        }
    }
    while (iRet < 0);
    return 0;
}

/*返回0，表示满足条件，返回-1，表示发现IsAbort为真了*/
int sem_wait_abort(sem_t * se, int * pIsAbort)
{
    struct timespec tmout;
    int iRet;

    do
    {
        tmout.tv_sec = time(NULL)+1;
        tmout.tv_nsec = 0;
        if (*pIsAbort)
        {
            return -1;
        }
        iRet = sem_timedwait(se, &tmout);
    }
    while (iRet < 0);
    return 0;
}

void * CompressThreadProc(void * pPara)
{
    COMPRESSTHREADPARA * pThreadPara = (COMPRESSTHREADPARA *)pPara;
    QUICKBUFFER * pQB = pThreadPara->pQB;
    int iBufFlag=0;
    int len;
    int iSBufOff;
    int iDBufOff;
    int iBufIdx;
    int iRet;
    //int iWaitRet;

    uLong destLen;
    uLong sourceLen;


    PrtLog(DEBUG0_LOG, "Thread %d compress data begin\n",pThreadPara->id);

    while (!(*pThreadPara->pIsAbort))
    {
        iBufIdx  = iBufFlag*pQB->BlkCnt+pThreadPara->id;
        iSBufOff = iBufIdx*pQB->BlkSz;
        iDBufOff = iBufIdx*(pQB->BlkSz + 1);

        /*
        iWaitRet = sem_wait_cond(&pQB->sem_srcbuf_havedata[iBufIdx],
                      pThreadPara->pReadIsOver,
                      pThreadPara->pIsAbort);

        if (iWaitRet) iWaitRet=-1异常退出,iWaitRet=1正常退出
        {
            break;
        }
        */

        iRet = sem_wait_abort(&pQB->sem_srcbuf_havedata[iBufIdx],
                              pThreadPara->pIsAbort);
        if (iRet < 0)
        {
            PrtLog(ERR_LOG,
                   "There was error in another thread, compress thread id %d will exit.\n",
                   pThreadPara->id);
            break;
        }

        iRet = sem_wait_abort(&pQB->sem_dstbuf_isempty[iBufIdx], pThreadPara->pIsAbort);
        if (iRet < 0) /*异常退出*/
        {
            PrtLog(ERR_LOG,
                   "There was error in another thread, compress thread id %d will exit.\n",
                   pThreadPara->id);
            break;
        }

        len = pQB->srcbuflen[iBufIdx];
        if (len > 0)
        {
            if (pThreadPara->zlevel > 0)
            {
                /*0x01表明是压缩的数据*/
                pQB->dstbuf[iDBufOff] = 0x01;
                sourceLen = len;
                destLen = pQB->BlkSz;

                iRet = compress2( (Bytef *)&pQB->dstbuf[iDBufOff + 1],
                                   &destLen,
                                   (const Bytef *)&pQB->srcbuf[iSBufOff],
                                   sourceLen,
                                   pThreadPara->zlevel
                                );
                pQB->dstbuflen[iBufIdx] = destLen;
            }
            else
            {
                iRet = -1;
            }

            if (iRet != Z_OK)
            {
                /*非压缩的数据*/
                pQB->dstbuflen[iBufIdx] = len;
                pQB->dstbuf[iDBufOff] = 0x00;
                memcpy(&pQB->dstbuf[iDBufOff+1],
                       &pQB->srcbuf[iSBufOff],
                       len);
                PrtLog(DEBUG1_LOG,
                       "Compress Thread id %d, not compress, data len=%d,iBufIdx=%d\n",
                       pThreadPara->id,
                       len,
                       iBufIdx
                       );
            }
            else
            {
                PrtLog(DEBUG1_LOG,
                       "Compress Thread id %d, in data len=%d, out data len=%d\n",
                       pThreadPara->id,
                       len,
                       pQB->dstbuflen[iBufIdx]);
            }

        }
        else
        {
            pQB->dstbuflen[iBufIdx] = len;
        }

        sem_post(&pQB->sem_srcbuf_isempty[iBufIdx]);
        sem_post(&pQB->sem_dstbuf_havedata[iBufIdx]);

        iBufFlag = !iBufFlag;
        if (len == -1) /*表示，没有数据了，结束了*/
        {
            break;
        }
    }

    PrtLog(DEBUG0_LOG,"Thread %d compress data end\n", pThreadPara->id);
    return 0;
}

int ReConnectToServer(const char * szDestIp, int iPort)
{
    int fd;
    int iMsgCode;
    int failedcnt;
    int iRet;
    char buf[256];

    failedcnt = 0;
    while (1)
    {
        fd = ConnectToServer(szDestIp, iPort, NET_TIMEOUT_SECOND*1000);
        if (fd <=0)
        {
            failedcnt++;
            if (failedcnt > NET_RETRY_CNT)
            {
                return -1;
            }
            sleep(10);
            continue;
        }

        /*前4个字节，是命令类型: RE_CONNECT_TAG*/
        SetIntMsg(buf, RE_CONNECT_TAG);
        /*后面,4个字符表示当前的clientid, 4个字节socket buffer size*/
        SetIntMsg(&buf[4], g_clientid);
        SetIntMsg(&buf[8], g_sock_buf_size);

        iRet=SendPacket(fd, buf, 12);
        if (iRet <0)
        {
            PrtLog(ERR_LOG, "Network error when send reconnect packet!\n");
            CloseSocketImmediate(fd);
            sleep(10);
            continue;
        }

        /*接收对方的响应包*/
        iRet=ReadPacket(fd, buf);
        if(iRet<=0)
        {
            PrtLog(ERR_LOG, "Network error when receive reconnect respond packet!\n");
            CloseSocketImmediate(fd);
            sleep(10);
            continue;
        }
        buf[iRet]=0;
        iMsgCode = GetIntMsg(buf);
        if (iMsgCode != 0)
        {
            PrtLog(ERR_LOG, "Server return error: %s\n",&buf[4]);
            CloseSocketImmediate(fd);
            return -1;
        }
        break;
    }
    return fd;
}

void PrintStat(SENDTHREADPARA * pThreadInfo)
{
    double SendSecs;
    int64_t nSendBytes;
    double ReadSecs;
    int64_t nReadBytes;

    GetRateStat(pThreadInfo->pSendRateStat, &SendSecs, &nSendBytes);
    if (SendSecs >= 2 && nSendBytes >0)
    {
        GetRateStatAndReset(pThreadInfo->pReadRateStat, &ReadSecs, &nReadBytes);
        RateStatReset(pThreadInfo->pSendRateStat);
        if (nReadBytes> 0 && ReadSecs > 0)
        {
            printf("----read speed: %lld/s, send speed: %lld/s, rate of compression : %d%%\n",
                 (long long int)(nReadBytes/ReadSecs),
                 (long long int)(nSendBytes/SendSecs),
                 (int)((nSendBytes/SendSecs)*100/(nReadBytes/ReadSecs))
               );
        }
    }
}

int SendBufferData(SENDTHREADPARA * pThreadInfo, int iBufFlag)
{
    QUICKBUFFER * pQB = pThreadInfo->pQB;
    int i;
    int iRet;
    int iMsgCode;
    char buf[1024];
    int iDBufOff;
    int iBufIdx;

    SetIntMsg(buf, SEQ_PACKET_TAG);
    SetI64Msg(&buf[4], pThreadInfo->seq);

    iRet = SendPacket(pThreadInfo->fd, buf, 12);
    if (iRet < 0)
    {
        return -1;
    }

    PrtLog(DEBUG1_LOG, "Send data begin, sequence=%d\n", pThreadInfo->seq);

    for (i=0; i<pQB->BlkCnt; i++)
    {
        iBufIdx  = iBufFlag*pQB->BlkCnt + i;
        iDBufOff = iBufIdx*(pQB->BlkSz+1);

        if (pQB->dstbuflen[iBufIdx] > 0)
        {
            iRet = SendPacket(pThreadInfo->fd,
                       &pQB->dstbuf[iDBufOff],
                       pQB->dstbuflen[iBufIdx]+1);
            if (iRet < 0)
            {
                return -1;
            }
            PrtLog(DEBUG1_LOG,
                   "Send data, length=%d,iBufIdx=%d\n",
                   pQB->dstbuflen[iBufIdx]+1,
                   iBufIdx
                   );
            RateStatAddValue(pThreadInfo->pSendRateStat, pQB->dstbuflen[iBufIdx]+1);
            (*pThreadInfo->pSendBytes) += pQB->dstbuflen[iBufIdx]+1;

        }
        else
        {
            /*发送0xFF，表明这个组合包结束了*/
            buf[0] = 0xFF;
            iRet = SendPacket(pThreadInfo->fd, buf, 1);
            if (iRet < 0)
            {
                return -1;
            }

            PrtLog(DEBUG1_LOG, "Send data(flag=0xFF), length=1\n");
            break;
        }
    }

    if (g_loglevel & STAT_LOG)
    {
        MeanTimeStatStart(&pThreadInfo->ts_respond);
    }

    iRet = ReadPacket(pThreadInfo->fd, buf);
    if (iRet < 0)
    {
        return -1;
    }

    if (g_loglevel & STAT_LOG)
    {
        MeanTimeStatEnd(&pThreadInfo->ts_respond, 20, "wait respond time");
    }

    iMsgCode = GetIntMsg(buf);
    if (iMsgCode != 0)
    {
        return -1;
    }


    PrtLog(DEBUG1_LOG, "Send data end, sequence=%d\n", pThreadInfo->seq);
    pThreadInfo->seq++;

    PrintStat(pThreadInfo);

    return 0;
}

int SendEndPacket(SENDTHREADPARA * pThreadInfo)
{
    int iRet;
    char buf[128];

    SetIntMsg(buf, END_PACKET_TAG);

    iRet = SendPacket(pThreadInfo->fd, buf, 4);
    if (iRet < 0)
    {
        return -1;
    }
    return 0;
}

int SendHeartBeatPacket(SENDTHREADPARA * pThreadPara)
{
    int iRet;
    char buf[8];

    SetIntMsg(buf, HEART_BEAT_PACKET_TAG);

    iRet = SendPacket(pThreadPara->fd, buf, 4);
    if (iRet < 0)
    {
        return -1;
    }
    return 0;
}


int WaitSendBufferHaveData(SENDTHREADPARA * pThreadPara, int iBufFlag)
{
    QUICKBUFFER  * pQB= pThreadPara->pQB;
    int i;
    sem_t * se;
    int iRet;
    struct timespec tmout;
    time_t starttime = time(NULL);

    for(i=0; i<pQB->BlkCnt; i++)
    {
        se = &pQB->sem_dstbuf_havedata[iBufFlag*pQB->BlkCnt+i];
        iRet = 0;
        do
        {
            tmout.tv_sec = time(NULL)+1;
            tmout.tv_nsec = 0;
            if ( tmout.tv_sec - 1 - starttime >= HEART_BEAT_INTERVAL)
            {
                SendHeartBeatPacket(pThreadPara);
                starttime = time(NULL);
            }

            if (*pThreadPara->pIsAbort)
            {
                return -1;
            }
            iRet = sem_timedwait(se, &tmout);
        }
        while (iRet < 0);
    }
    return 0;
}

void * SendDataThreadProc(void * pPara)
{
    SENDTHREADPARA * pThreadPara = (SENDTHREADPARA *)pPara;
    QUICKBUFFER  * pQB= pThreadPara->pQB;
    int iBufFlag=0;
    int i;
    int iRet;
    int iNetCode;
    int iIsExit = 0;
    int iWaitRet;
    int len;

    PrtLog(DEBUG0_LOG, "Senddata thread start...\n");

    while(!(*pThreadPara->pIsAbort))
    {
        iWaitRet =  WaitSendBufferHaveData(pThreadPara, iBufFlag);
        if (iWaitRet == -1)
        {
            PrtLog(ERR_LOG,
                   "There was error in another thread, send thread id will exit.\n");
            iIsExit = 1;
            break;
        }

        len = pQB->dstbuflen[iBufFlag*pQB->BlkCnt];
        if (len != -1)
        {
            if (g_loglevel & STAT_LOG)
            {
                MeanTimeStatStart(&pThreadPara->ts_send);
            }

            do
            {
                iRet = SendBufferData(pThreadPara, iBufFlag);
                if (iRet < 0 )
                {
                    CloseSocketImmediate(pThreadPara->fd);
                    sleep(5);
                    PrtLog(ERR_LOG, "Network error, reconnect...\n");
                    iNetCode = ReConnectToServer(pThreadPara->servip, pThreadPara->port);
                    if (iNetCode < 0) /*网络始终连接不上或发生错误*/
                    {
                        PrtLog(ERR_LOG, "Can not reconnect to server, program will exit.\n");
                        *pThreadPara->pIsAbort = 1;
                        break;
                    }
                    pThreadPara->fd = iNetCode;
                    PrtLog(INFO_LOG, "Reconnect OK\n");
                }
            }while (iRet < 0);

            if (g_loglevel & STAT_LOG)
            {
                MeanTimeStatEnd(&pThreadPara->ts_send, 20, "send chunk time");
            }
        }

        for (i=0; i<pQB->BlkCnt; i++)
        {
            sem_post(&pQB->sem_dstbuf_isempty[iBufFlag*pQB->BlkCnt+i]);
        }

        /* 长度为-1，表示结束了*/
        if (len == -1)
        {
            break;
        }

        iBufFlag = !iBufFlag;
    }

    /*发送结束的标记包*/
    iRet =  SendEndPacket(pThreadPara);

    PrtLog(DEBUG0_LOG, "Senddata thread exit.\n");
    return 0;
}


/*******************************serv program****************************/

int InitJobSlot()
{
    pthread_mutexattr_t attr;
    int memsize = sizeof(SERVERJOBTHREADDATA)*MAX_PARALLEL_JOB;
    g_serv_job_data = (SERVERJOBTHREADDATA*) malloc(memsize);
    memset(g_serv_job_data, 0, memsize);
    pthread_mutexattr_init(&attr);
    pthread_mutex_init(&g_serv_job_data_mutex, &attr);
    return 0;
}
int FreeJobSlot()
{
    free(g_serv_job_data);
    pthread_mutex_destroy(&g_serv_job_data_mutex);
    return 0;
}

SERVERJOBTHREADDATA * GetJobSlot()
{
    int i;
    pthread_mutexattr_t attr;

    pthread_mutex_lock(&g_serv_job_data_mutex);
    for (i=0; i<MAX_PARALLEL_JOB; i++)
    {
        if (! g_serv_job_data[i].isused)
        {
            g_serv_job_data[i].isused = 1;
            g_serv_job_data[i].szCmd = malloc(CMD_MAX_LENGTH);
            g_clientid_seq++;
            g_serv_job_data[i].clientid = g_clientid_seq;

            pthread_mutexattr_init(&attr);
            pthread_mutex_init(&g_serv_job_data[i].m_mutex, &attr);
            g_serv_job_data[i].refd = -1;

            pthread_mutex_unlock(&g_serv_job_data_mutex);
            return &g_serv_job_data[i];
        }
    }
    pthread_mutex_unlock(&g_serv_job_data_mutex);
    return NULL;
}

void ReleaseJobSlot(SERVERJOBTHREADDATA * pServJob)
{

    pthread_mutex_lock(&g_serv_job_data_mutex);
    pServJob->isused = 0;
    if (pServJob->szCmd != NULL)
    {
        free(pServJob->szCmd);
        pServJob->szCmd = NULL;
    }
    pthread_mutex_destroy(&pServJob->m_mutex);

    pthread_mutex_unlock(&g_serv_job_data_mutex);
    return;
}


int SetJobNetOk(int id, int fd)
{
    int i;
    pthread_mutex_lock(&g_serv_job_data_mutex);
    for (i=0; i<MAX_PARALLEL_JOB; i++)
    {
        if (g_serv_job_data[i].isused && g_serv_job_data[i].clientid == id)
        {
            break;
        }
    }
    if (i == MAX_PARALLEL_JOB)
    {
        pthread_mutex_unlock(&g_serv_job_data_mutex);
        return -1;
    }
    /*CloseSocketImmediate(g_serv_job_data[i].fd);*/

    pthread_mutex_lock(&g_serv_job_data[i].m_mutex);
    if (g_serv_job_data[i].refd != -1)
    {
        close(g_serv_job_data[i].refd);
    }
    g_serv_job_data[i].refd = fd;
    pthread_mutex_unlock(&g_serv_job_data[i].m_mutex);

    pthread_mutex_unlock(&g_serv_job_data_mutex);
    return 0;
}


int SocketListen(int iPort)
{
    int fd;
    struct sockaddr_in my_addr;
    struct linger li;
    int iRet;

    //0.init
    memset ((char *)&my_addr, 0, sizeof(struct sockaddr_in));
    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(iPort);
    my_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    //1.create
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1)
    {
        return -1;
    }

    li.l_onoff = 1;
    li.l_linger = 0;

    int option=1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&option, sizeof(option));
    setsockopt(fd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof(li));

    //2.bind
    iRet = bind(fd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr));
    if (iRet==-1)
    {
        PrtLog(ERR_LOG, "bind %d port error\n",iPort);
        return -1;
    }

    //3.listen
    iRet = listen(fd, 10);
    if (iRet==-1)
    {
        PrtLog(ERR_LOG, "listen in port %d error!\n",iPort);
        close(fd);
        return -1;
    }
    return fd;
}

int CheckIpPermission(struct sockaddr_in * pIp, PROG_ARGS * qpipe_args)
{
    int i;
    unsigned int netmask;

    for (i=0; i<qpipe_args->netcnt; i++)
    {
        netmask = GetMaskByLen(qpipe_args->masklen[i]);
        netmask = htonl(netmask);

        if (((pIp->sin_addr.s_addr)&netmask) == ((qpipe_args->netip[i]) &netmask))
        {
            return 1;
        }
    }
    return 0;
}


int RunServer(PROG_ARGS * qpipe_args)
{
    int nRet;
    int fromclientfd;
    char retbuf[256];
    char buf[CMD_MAX_LENGTH+128];
    int len;
    int iMsgCode;
    int iPacketLen;
    char ipstr[INET_ADDRSTRLEN];


    set_signal();

    g_listensock =  SocketListen(qpipe_args->port);
    if ( g_listensock< 0)
    {
        return -1;
    }

    InitJobSlot();

    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);

    while (!g_iIsAbort)
    {
        fromclientfd = accept(g_listensock,(struct sockaddr *)&from,&fromlen);
        if (fromclientfd == -1)
        {
            if (g_listensock != -1)
            {
                close(g_listensock);
            }
            PrtLog(INFO_LOG, "Server is stoped.\n");
            break;
        }

        OptimizedSocket(fromclientfd, 65536);

        /*第一个数据包内容：4个字节，命令类型*/
        iPacketLen = ReadPacket(fromclientfd, buf);
        if (iPacketLen<0)
        {
            close(fromclientfd );
            continue;
        }
        buf[iPacketLen]=0;

        if ( !CheckIpPermission(&from, qpipe_args) )
        {
            /*返回错误信息的数据包给客户端*/
            SetIntMsg(retbuf, -1);
            strcpy(&retbuf[4], "Client does not allow access to the server!");
            len = strlen(&retbuf[4])+4;
            SendPacket(fromclientfd, retbuf, len);
            close(fromclientfd);
            continue;
        }

        inet_ntop(AF_INET, &(from.sin_addr), ipstr, INET_ADDRSTRLEN);

        iMsgCode = GetIntMsg(buf);
        if (iMsgCode == NEW_CONNECT_TAG)
        {
            PrtLog(INFO_LOG, "Receive a new job connection from %s:%u, sockfd=%d ...\n",
                   ipstr, from.sin_port, fromclientfd);

            /*后4个字节是threadnums，4个字节BlkSz，后面是命令*/
            SERVERJOBTHREADDATA * pJobData = GetJobSlot();
            if (pJobData == NULL)
            {
                PrtLog(ERR_LOG, "Too many job running in server!\n");

                /*返回错误信息的数据包给客户端*/
                SetIntMsg(retbuf, -1);
                strcpy(&retbuf[4], "Too many job running in server!");
                len = strlen(&retbuf[4])+4;
                SendPacket(fromclientfd, retbuf, len);
                close(fromclientfd);
                continue;
            }

            pJobData->threadnums = GetIntMsg(&buf[4]);
            pJobData->BlkSz = GetIntMsg(&buf[8]);
            //printf("socket buffer size =%d\n", GetIntMsg(&buf[12]));

            OptimizedSocket(fromclientfd, GetIntMsg(&buf[12]));

            PrtLog(INFO_LOG, "allocate a new client(%d)\n", pJobData->clientid);

            strcpy(pJobData->szCmd, &buf[16]);
            PrtLog(INFO_LOG, "client(%d), cmd : %s \n", pJobData->clientid, pJobData->szCmd);

            pJobData->PacketSeq = 1;

            pJobData->fd = fromclientfd;
            pthread_attr_init(&pJobData->tattr);
            pthread_attr_setdetachstate(&pJobData->tattr,PTHREAD_CREATE_DETACHED);
            pthread_create (&pJobData->tid, &pJobData->tattr, ServerJobThreadProc, (void *)pJobData);
        }
        else if (iMsgCode == RE_CONNECT_TAG)
        {
            int RecvClientId = GetIntMsg(&buf[4]);
            PrtLog(INFO_LOG, "Receive a reconnect request from %s:%u , clent id =%d, sockfd=%d\n", 
                   ipstr, from.sin_port, RecvClientId, fromclientfd);
            OptimizedSocket(fromclientfd, GetIntMsg(&buf[8]));

            nRet = SetJobNetOk(RecvClientId, fromclientfd);
            if (nRet < 0 )
            {
                /*返回错误信息的数据包给客户端*/
                SetIntMsg(retbuf,-1);
                sprintf(&retbuf[4], "Server already close this client(%d)!", RecvClientId);
                len = strlen(&retbuf[4])+4;
                SendPacket(fromclientfd, retbuf, len);
                close(fromclientfd);
                PrtLog(INFO_LOG, "Server already close this client(%d)!\n", RecvClientId);
            }
            else
            {
                /*返回成功的数据包给客户端*/
                SetIntMsg(retbuf, 0);
                SendPacket(fromclientfd, retbuf, 4);
                /*PrtLog(INFO_LOG, "client(%d) reconnected\n", RecvClientId);*/
            }
        }
    }
    return 0;
}

/*
返回-1，表示网络中断，需要关闭这个任务，返回0，表示成功，返回1，表示结束
 */
int RecvData(SERVERJOBTHREADDATA * pJobPara, int iBufFlag, uint64_t * pRecvSeq)
{
    int i;
    char buf[1024];
    int iPacketLen;
    QUICKBUFFER * pQB = pJobPara->pQB;
    int iSBufOff;
    int iBufIdx;
    int iCmdCode;
    int iNetCode;

    do
    {
        iPacketLen = ReadPacket(pJobPara->fd, buf);
        if (iPacketLen < 0)
        {
            return -1;
        }
        iCmdCode = GetIntMsg(buf);
        if (iCmdCode == HEART_BEAT_PACKET_TAG)
        {
            PrtLog(DEBUG0_LOG, "client(%d), recv a heart beat packet\n", pJobPara->clientid);
        }
    }
    while (iCmdCode == HEART_BEAT_PACKET_TAG);

    if (iCmdCode == END_PACKET_TAG) /*数据结束*/
    {
        PrtLog(DEBUG0_LOG, "client(%d), recv END_PACKET_TAG\n", pJobPara->clientid);
        for (i=0; i<pJobPara->threadnums; i++)
        {
            iBufIdx  = iBufFlag*pQB->BlkCnt + i;
            pQB->srcbuflen[iBufIdx] = -1; /*-1表示数据结束*/
        }
        return 1;
    }

    *pRecvSeq = GetI64Msg(&buf[4]);
    PrtLog(DEBUG1_LOG, "client(%d), recv a packet begin, sequence=%lld\n",
           pJobPara->clientid, (long long)(*pRecvSeq) );

    for (i=0; i<pJobPara->threadnums; i++)
    {
        iBufIdx  = iBufFlag*pQB->BlkCnt + i;
        pQB->srcbuflen[iBufIdx] = 0;
    }

    for (i=0; i<pJobPara->threadnums; i++)
    {
        iBufIdx  = iBufFlag*pQB->BlkCnt + i;
        iSBufOff = iBufIdx*(pQB->BlkSz+1);
        iPacketLen = ReadPacket(pJobPara->fd, &pQB->srcbuf[iSBufOff]);
        if (iPacketLen <0)
        {
            return -1;
        }

        PrtLog(DEBUG1_LOG, "client(%d) recv data, length=%d\n", pJobPara->clientid, iPacketLen);

        /*第一个字节是0xff，表示结束了*/
        if ((unsigned char )pQB->srcbuf[iSBufOff] == 0xFF)
        {
            pQB->srcbuflen[iBufIdx] = 0;
            break;
        }
        pQB->srcbuflen[iBufIdx] = iPacketLen;
    }

    /*发送标志给client，表明成功处理这个包。*/

    SetIntMsg(buf, 0);
    iNetCode = SendPacket(pJobPara->fd, buf, 4);
    if (iNetCode < 0)
    {
        ;
        /*return -1; 这时候出错，数据包还是正常接收到了，只是回响应包丢失了*/
    }

    PrtLog(DEBUG1_LOG, "client(%d) recv a packet end, sequence=%lld\n", pJobPara->clientid, (long long )(*pRecvSeq) );

    return 0;
}

/*
返回-1，表示网络中断，返回-2，表示sequence错误，需要关闭这个任务，返回0，表示成功，返回1，表示结束
 */
int RecvDataIgnoreDupPacket(SERVERJOBTHREADDATA * pJobPara, int iBufFlag)
{
    int iRet;
    uint64_t RecvSeq;

    while (1)
    {
        iRet = RecvData(pJobPara, iBufFlag, &RecvSeq);
        if (iRet < 0)
        {
            return -1;
        }
        else if(iRet == 1) /*  data is over */
        {
            return 1;
        }

        if (RecvSeq == pJobPara->PacketSeq)
        {
            break;
        }
        else if (RecvSeq < pJobPara->PacketSeq)
        {
            PrtLog(INFO_LOG,
                   "client(%d) recv a duplicate packet(%lld), need packet(%lld), ignore it.\n",
                   pJobPara->clientid,
                   (long long )RecvSeq,
                   (long long )pJobPara->PacketSeq);
            continue;
        }
        else
        {
            PrtLog(ERR_LOG,
                   "client(%d) recv a packet(%lld), but need packet(%lld), may some packet lost.\n",
                   pJobPara->clientid,
                   (long long )RecvSeq,
                   (long long )pJobPara->PacketSeq);

            return -2;
        }
    }

    pJobPara->PacketSeq++;
    return 0;
}

void * ServerJobThreadProc(void * pPara)
{
    SERVERJOBTHREADDATA * pJobPara = (SERVERJOBTHREADDATA *)pPara;
    char retbuf[1024];
    int iRet;
    int len;
    int i;
    int iBufFlag;
    int IsAbort = 0;
    int iIsExit = 0;
    int ReadIsOver = 0;
    int UnCompressIsOver = 0;
    FILE * fp;
    QUICKBUFFER * pQB;

    int NetBreakRetryCnt = 0;

    PrtLog(DEBUG1_LOG, "client(%d), server job thread start...\n", pJobPara->clientid);

    fp = popen(pJobPara->szCmd, "w");
    if (fp == NULL)
    {
        retbuf[0]=1;
        retbuf[1]=0;
        strerror_r(errno,&retbuf[1],1020);
        len=1+strlen(&retbuf[1]);
        SendPacket(pJobPara->fd, retbuf, len);
        close(pJobPara->fd);
        free(pJobPara->szCmd);
        pJobPara->isused = 0;
        return NULL;
    }


    /*返回创建成功的数据包给客户端*/
    SetIntMsg(retbuf, 0);
    SetIntMsg(&retbuf[4], pJobPara->clientid);
    SendPacket(pJobPara->fd, retbuf, 8);

    pQB =  AllocQuickBuffer(pJobPara->threadnums, pJobPara->BlkSz);
    pJobPara->pQB = pQB;

    pJobPara->UnCompressThreadData = malloc(sizeof(UNCOMPRESSTHREADPARA)*pJobPara->threadnums);
    /*创建解压数据的线程*/
    for(i=0; i<pJobPara->threadnums; i++)
    {
        pJobPara->UnCompressThreadData[i].id=i;
        pJobPara->UnCompressThreadData[i].pQB=pQB;
        pJobPara->UnCompressThreadData[i].pIsAbort = &IsAbort;
        pJobPara->UnCompressThreadData[i].pReadIsOver = &ReadIsOver;
        pJobPara->UnCompressThreadData[i].clientid = pJobPara->clientid;

        pthread_attr_init(&pJobPara->UnCompressThreadData[i].tattr);
        pthread_create (&pJobPara->UnCompressThreadData[i].tid,
                        &pJobPara->UnCompressThreadData[i].tattr,
                        UnCompressThreadProc,
                        (void *)&pJobPara->UnCompressThreadData[i]);
    }

    /*创建把数据写入文件的线程*/
    WRITETHREADPARA WriteThreadPara;
    WriteThreadPara.pIsAbort = &IsAbort;
    WriteThreadPara.pUnCompressIsOver = &UnCompressIsOver;
    WriteThreadPara.fp = fp;
    WriteThreadPara.pQB = pQB;
    WriteThreadPara.clientid = pJobPara->clientid;


    pthread_attr_init(&WriteThreadPara.tattr);
    pthread_create(&WriteThreadPara.tid,
                   &WriteThreadPara.tattr,
                   WriteDataThreadProc,(void *)&WriteThreadPara);

    iBufFlag=0;
    while (!iIsExit)
    {
        for(i=0; i<pQB->BlkCnt; i++)
        {
            iRet = sem_wait_abort(&pQB->sem_srcbuf_isempty[iBufFlag*pQB->BlkCnt+i], &IsAbort);
            if (iRet == -1) /*发生异常*/
            {
                break;
            }
        }

        if (IsAbort)
        {
            break;
        }

retry_recv_data:
        iRet = RecvDataIgnoreDupPacket(pJobPara, iBufFlag);
        if (iRet == -1 ) /* 发生网络错误 */
        {
            PrtLog(ERR_LOG,
                   "client(%d) : network error, wait for reconnect ...\n",
                   pJobPara->clientid);

            while ( NetBreakRetryCnt * 10 < g_serv_wait_reconnect_time)
            {
                pthread_mutex_lock(&pJobPara->m_mutex);
                if (pJobPara->refd != -1)
                {
                    close(pJobPara->fd);
                    pJobPara->fd = pJobPara->refd;
                    pJobPara->refd = -1;
                    pthread_mutex_unlock(&pJobPara->m_mutex);
                    PrtLog(INFO_LOG,
                           "client(%d) : reconnect ok.\n",
                           pJobPara->clientid);
                    NetBreakRetryCnt = 0;
                    goto retry_recv_data;
                }
                else
                {
                    NetBreakRetryCnt ++ ;
                }
                pthread_mutex_unlock(&pJobPara->m_mutex);
                sleep(10);
            }

            PrtLog(ERR_LOG,
                   "client(%d) : server wait reconnect timeout, close it!!!\n",
                    pJobPara->clientid);
            iIsExit = 1;
            IsAbort = 1;
            break;

        }
        else if (iRet == -2) /*接收到的数据包的seq错误，退出任务*/
        {
            iIsExit = 1;
            IsAbort = 1;
        }
        else if (iRet == 1)/*接收数据结束*/
        {
            iIsExit = 1;
        }

        for(i=0; i<pQB->BlkCnt; i++)
        {
            sem_post(&pQB->sem_srcbuf_havedata[iBufFlag*pQB->BlkCnt+i]);
        }

        iBufFlag=!iBufFlag;
    }

    ReadIsOver = 1;

    PrtLog(DEBUG0_LOG, "client(%d), recv data is over.\n", pJobPara->clientid);

    for(i=0;i<pJobPara->threadnums;i++)
    {
        pthread_join(pJobPara->UnCompressThreadData[i].tid, NULL);
    }
    UnCompressIsOver = 1;

    pthread_join(WriteThreadPara.tid, NULL);

    FreeQuickBuffer(pQB);

    free(pJobPara->UnCompressThreadData);
    close(pJobPara->fd);
    ReleaseJobSlot(pJobPara);

    return 0;
}


void * UnCompressThreadProc(void * pPara)
{
    UNCOMPRESSTHREADPARA * pThreadPara = (UNCOMPRESSTHREADPARA *)pPara;
    QUICKBUFFER * pQB = pThreadPara->pQB;
    int iBufFlag = 0;
    int len;
    int iSBufOff;
    int iDBufOff;
    int iBufIdx;
    //int iWaitRet;
    int iRet;
    uLong destLen;
    uLong sourceLen;

    while (1)
    {
        iBufIdx  = iBufFlag*pQB->BlkCnt+pThreadPara->id;
        iSBufOff = iBufIdx*(pQB->BlkSz+1);
        iDBufOff = iBufIdx*(pQB->BlkSz);


        iRet = sem_wait_abort(&pQB->sem_srcbuf_havedata[iBufIdx],
                              pThreadPara->pIsAbort);
        if (iRet < 0)
        {
            PrtLog(ERR_LOG,
                   "client(%d), there was error in another thread, uncompress thread id %d will exit.\n",
                   pThreadPara->clientid,
                   pThreadPara->id);
            break;
        }

        iRet = sem_wait_abort(&pQB->sem_dstbuf_isempty[iBufIdx], pThreadPara->pIsAbort);
        if (iRet < 0) /*异常退出*/
        {
            PrtLog(ERR_LOG,
                   "client(%d) ,there was error in another thread, compress thread id %d will exit.\n",
                   pThreadPara->clientid,
                   pThreadPara->id);
            break;
        }

        len = pQB->srcbuflen[iBufIdx];
        if (len>1)
        {
            pQB->dstbuflen[iBufIdx] = pQB->BlkSz;
            if (pQB->srcbuf[iSBufOff] ==0x00) /*非压缩的数据*/
            {
                memcpy(&pQB->dstbuf[iDBufOff],
                       &pQB->srcbuf[iSBufOff+1],
                       len -1
                       );
                pQB->dstbuflen[iBufIdx] = len -1;

                PrtLog(DEBUG1_LOG, "client(%d), UnCompress Thread id %d, not compress, out data len=%d\n",
                    pThreadPara->clientid,
                    pThreadPara->id,
                    pQB->dstbuflen[iBufIdx]);
            }
            else if ((unsigned char)pQB->srcbuf[iSBufOff] ==0xFF) /*第一个字节是0xff，表示结束了*/
            {
                pQB->dstbuflen[iBufIdx]=0;
            }
            else
            {
                destLen = pQB->BlkSz;
                sourceLen = len -1;
                uncompress((Bytef *)&pQB->dstbuf[iDBufOff],
                              &destLen,
                              (const Bytef *)&pQB->srcbuf[iSBufOff+1],
                               sourceLen
                           );
                pQB->dstbuflen[iBufIdx] = destLen;

                PrtLog(DEBUG1_LOG,
                       "client(%d), UnCompress Thread id %d, in data len=%d, out data len=%d\n",
                       pThreadPara->clientid,
                       pThreadPara->id,
                       len,
                       pQB->dstbuflen[iBufIdx]);
            }


        }
        else
        {
            if (len>=0)
            {
                pQB->dstbuflen[iBufIdx]=0;
            }
            else
            {
                pQB->dstbuflen[iBufIdx]=-1;
            }
        }

        sem_post(&pQB->sem_srcbuf_isempty[iBufIdx]);
        sem_post(&pQB->sem_dstbuf_havedata[iBufIdx]);
        iBufFlag = !iBufFlag;
        if (len == -1)
        {
            break;
        }
    }

    PrtLog(DEBUG0_LOG,
           "client(%d), Uncompress thread %d exit.\n",
           pThreadPara->clientid,
           pThreadPara->id);
    return 0;
}


void * WriteDataThreadProc(void * pPara)
{
    WRITETHREADPARA * pThreadPara = (WRITETHREADPARA *)pPara;
    QUICKBUFFER * pQB = pThreadPara->pQB;
    int iBufFlag=0;
    int i;
    int iIsExit=0;
    int iWaitRet;
    int iBufIdx;
    int iDBufOff;
    int iRet;
    char szErrMsg[512];
    off_t write_nbytes=0;

    PrtLog(DEBUG0_LOG, "client(%d), Write thread start...\n", pThreadPara->clientid);

    while(!iIsExit)
    {

        for(i=0; i<pQB->BlkCnt; i++)
        {
            iWaitRet = sem_wait_abort(&pQB->sem_dstbuf_havedata[iBufFlag*pQB->BlkCnt+i],
                                 pThreadPara->pIsAbort);

            if (iWaitRet == -1)
            {
                PrtLog(ERR_LOG,
                       "client(%d), there was error in another thread, send thread id will exit.\n",
                       pThreadPara->clientid
                       );
                iIsExit = 1;
                break;
            }

        }

        if (iIsExit)
        {
            break;
        }

        for(i=0; i<pQB->BlkCnt; i++)
        {
            iBufIdx  = iBufFlag*pQB->BlkCnt + i;
            iDBufOff = iBufIdx*pQB->BlkSz;

            if(pQB->dstbuflen[iBufIdx]>0)
            {
                iRet = fwrite(&pQB->dstbuf[iDBufOff],
                      pQB->dstbuflen[iBufIdx],
                      1,
                      pThreadPara->fp
                      );
                if (iRet != 1)
                {
                    GetSysErrorInfo(errno, szErrMsg);
                    PrtLog(ERR_LOG,
                           "client(%d), write data error : %s\n",
                           pThreadPara->clientid,
                           szErrMsg);
                    *pThreadPara->pIsAbort = 1;
                    iIsExit = 1;
                    break;
                }

                write_nbytes += pQB->dstbuflen[iBufIdx];

                PrtLog(DEBUG1_LOG,
                       "client(%d), write data, length=%d,total write bytes=%lld\n",
                       pThreadPara->clientid,
                       pQB->dstbuflen[iBufIdx],
                       write_nbytes
                       );
            }
            else
            {
                if (pQB->dstbuflen[iBufIdx] == -1) /*结束，退出*/
                {
                    iIsExit = 1;
                }
                break;
            }
        }

        for(i=0; i<pQB->BlkCnt; i++)
        {
            sem_post(&pQB->sem_dstbuf_isempty[iBufFlag*pQB->BlkCnt+i]);
        }
        iBufFlag = !iBufFlag;
    }

    iRet = pclose(pThreadPara->fp);
    if (iRet != 0)
    {
        PrtLog(ERR_LOG,
               "client(%d), pclose return is %d, errors that may occur!\n",
               pThreadPara->clientid,
               iRet);
    }

    PrtLog(INFO_LOG,
           "client(%d), total receive bytes: %lld\n",
           pThreadPara->clientid, write_nbytes);

    PrtLog(DEBUG0_LOG, "client(%d), write thread exit!\n", pThreadPara->clientid);

    return 0;
}


void set_signal()
{
    signal(SIGHUP, signal_func);
    signal(SIGQUIT, signal_func);
    signal(SIGBUS, SIG_DFL);

    signal(SIGURG,signal_func);  /*软件中断。如当Alarm Clock超时（SIGURG），*/

    /*此程序必须忽略此事件,因为在socket写数据时,客户端可能把这个通信管道关闭.*/
    signal(SIGPIPE,SIG_IGN); /*当Reader中止之后又向管道写数据（SIGPIPE），*/

    signal(SIGABRT,SIG_IGN); /*由调用abort函数产生，进程非正常退出*/

    signal(SIGTRAP,SIG_IGN);

    signal(SIGILL,signal_func); /*非法指令异常*/
    /*signal(SIGSEGV,signal_func); 非法内存访问*/

    /*signal(SIGCHLD,SIG_IGN);//忽略SIGCHLD，使用子进程结束后，不会变成僵尸进程*/

    signal(SIGTERM,signal_func); /*收到kill后，把所有子进程kill掉*/
    signal(SIGINT, signal_func); /*Ctrl+C*/
}

void signal_func(int no)
{
    switch (no)
    {
    case 1:
        PrtLog(INFO_LOG, "Receive signal SIGHUP.\n");
        break;
    case SIGINT:
        PrtLog(INFO_LOG, "Receive Ctrl+C or signal SIGINT, cmdserv is stoping....\n");
        g_iIsAbort = 1;
        if (g_listensock != -1)
        {
            close(g_listensock);
            g_listensock = -1;
        }
        break;
    case SIGTERM:
        PrtLog(INFO_LOG, "Receive kill signal,Server is stoping...\n");
        g_iIsAbort = 1;
        if (g_listensock != -1)
        {
            close(g_listensock);
            g_listensock = -1;
        }
        break;
    case SIGQUIT:
        PrtLog(INFO_LOG, "Receive SIGQUIT signal.\n");
        break;

    case SIGABRT:
        PrtLog(INFO_LOG, "Receive SIGABRT signal.\n");
        break;

    case SIGILL:
        PrtLog(INFO_LOG, "Receive SIGILL signal.\n");
        break;

    case SIGSEGV:
        PrtLog(INFO_LOG, "Receive SIGSEGV signal.\n");
        g_iIsAbort = 1;
        break;

    case SIGPIPE:
        PrtLog(INFO_LOG, "Get SIGPIPE signal.\n");
        break;

    default:
        PrtLog(INFO_LOG, "GET %d sigial!\n",no);
        break;
    break;
    }
}

int ReadPacket(int sockfd, char * p)
{
    int header;
    int len;
    int iRet;

    iRet = SocketRead(sockfd, (char *)&header, 4);
    if (iRet<0)
    {
        return iRet;
    }
    len = ntohl(header);
    if (len<=0)
    {
        return -1;
    }
    iRet = SocketRead(sockfd, p, len);
    return iRet;
}

int SocketRead(int sockfd, char * p, int len)
{
    int n=0;
    int readnums;
    char szErrInfo[512];
    int iRet;
    double start_time, end_time;
    int firstflag = 1;

    while (n<len)
    {
        iRet = TestCanRecv(sockfd, NET_TIMEOUT_SECOND*1000, szErrInfo);
        if (iRet <=0)
        {
            return -1;
        }

        readnums = recv(sockfd, &p[n], len -n, 0);
        if(readnums <= 0)
        {
            if( (errno==EINTR) || (errno==EWOULDBLOCK) )
            {
                if (firstflag)
                {
                    firstflag = 0;
                    start_time = time_so_far();
                    continue;
                }

                end_time = time_so_far();
                if ( (end_time - start_time) > NET_TIMEOUT_SECOND )
                {
                    return -1;
                }
                usleep(1000);
                continue;
            }
            else
            {
                return -1;
            }
        }
        n+=readnums;
    }
    return n;
}

int SendPacket(int sockfd,char * p,int len)
{
    int header;
    int iRet;
    header=htonl(len);
    iRet=SocketWrite(sockfd,(char *)&header,4);
    if(iRet<0)
    {
        return iRet;
    }
    iRet=SocketWrite(sockfd,(char *)p,len);
    return iRet;
}

int SocketWrite(int sockfd, char * p, int len)
{
    int n=0;
    int sendnums;
    int leftsize;
    int sendsize;
    char szErrInfo[512];
    int iRet;

    while(n<len)
    {
        leftsize = len - n;
        if (leftsize > g_sock_buf_size)
        {
            sendsize = g_sock_buf_size;
        }
        else
        {
            sendsize = leftsize;
        }

        iRet = TestCanSend(sockfd, NET_TIMEOUT_SECOND*1000 , szErrInfo);
        if (iRet <=0)
        {
            return -1;
        }

        sendnums = send(sockfd,&p[n], sendsize, 0);
        if (sendnums <= 0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            else if(errno == EWOULDBLOCK)
            {
                continue;
            }
            else
            {
                return -1;
            }
        }
        n+=sendnums;
    }
    return n;
}

/*测试socket是否有数据到达了，返回0表示超时，返回1表示可以读数据了，返回-1表示网络错误 */
int TestCanRecv(int sockfd, int timeoutms, char * szErrInfo)
{
    if (sockfd<0)
    {
        return -1;
    }

    int nfds;

    nfds = sockfd+1;

    fd_set  arfds,rfds;
    FD_ZERO(&arfds);
    FD_SET( sockfd ,&arfds);

    fd_set  aerrfds,errfds;
    FD_ZERO(&aerrfds);
    FD_SET( sockfd ,&aerrfds);

    struct timeval  timeout;
    if (timeoutms!=-1)
    {
        timeout.tv_sec = timeoutms / 1000;
        timeout.tv_usec = (timeoutms % 1000) * 1000;
    }

    int ccode=0;
    while(1)
    {
        memcpy(&rfds,&arfds,sizeof(rfds));
        memcpy(&errfds,&aerrfds,sizeof(errfds));

        if(timeoutms == -1)
        {
            ccode = select(nfds,&rfds,(fd_set *)0,&errfds,(struct timeval *)0);
        }
        else
        {
            ccode = select(nfds,&rfds,(fd_set *)0,&errfds,&timeout);
        }

        //超时
        if(ccode == 0)
        {
            return 0;
        }

        if(ccode < 0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            GetSysErrorInfo(errno,szErrInfo);
            return -1;
        }

        //读失败
        if(FD_ISSET(sockfd ,&errfds))
        {
            GetSysErrorInfo(errno,szErrInfo);
            return -1;
        }

        if(FD_ISSET(sockfd ,&rfds))
        {
            return 1;
        }
    }
}

/*测试socket是否能发送数据，返回0表示超时，返回1表示可以发送数据了，返回-1表示网络错误 */
int TestCanSend(int sockfd, int timeoutms, char * szErrInfo)
{
    if (sockfd<0)
    {
        return -1;
    }

    int nfds;

    nfds = sockfd+1;

    fd_set  awfds,wfds;
    FD_ZERO(&awfds);
    FD_SET( sockfd ,&awfds);

    fd_set  aerrfds,errfds;
    FD_ZERO(&aerrfds);
    FD_SET( sockfd ,&aerrfds);

    struct timeval  timeout;
    if (timeoutms!=-1)
    {
        timeout.tv_sec = timeoutms / 1000;
        timeout.tv_usec = (timeoutms % 1000) * 1000;
    }

    int ccode=0;
    while(1)
    {
        memcpy(&wfds,&awfds,sizeof(wfds));
        memcpy(&errfds,&aerrfds,sizeof(errfds));

        if(timeoutms == -1)
        {
            ccode = select(nfds,(fd_set *)0,&wfds,&errfds,(struct timeval *)0);
        }
        else
        {
            ccode = select(nfds,(fd_set *)0,&wfds,&errfds,&timeout);
        }

        //超时
        if(ccode == 0)
        {
            return 0;
        }

        if(ccode < 0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            GetSysErrorInfo(errno,szErrInfo);
            return -1;
        }

        //读失败
        if(FD_ISSET(sockfd ,&errfds))
        {
            GetSysErrorInfo(errno,szErrInfo);
            return -1;
        }

        if(FD_ISSET(sockfd ,&wfds))
        {
            return 1;
        }
    }
}


void GetSysErrorInfo(int err_no,char * szErrInfo)
{
    int headlen;
    //memset(szErrMsg,0,SYS_ERRMSG_MAX_SIZE);
    headlen=sprintf(szErrInfo,"errno=%d, ",err_no);
    strerror_r(err_no,&szErrInfo[headlen],(size_t)SYS_ERRMSG_MAX_SIZE);
}

void set_nonblock(int socket)
{
    int flags;
    flags = fcntl(socket,F_GETFL,0);
    /*assert(flags != -1);*/
    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

static double time_so_far()
{
#if defined(SysV)
    int        val;
    struct tms tms;

    if ((val = times(&tms)) == -1)
    {
        printf("Call times() error\n");
    }
    return ((double) val) / ((double) sysconf(_SC_CLK_TCK));

#else

    struct timeval tp;

    if (gettimeofday(&tp, (struct timezone *) NULL) == -1)
    {
        printf("Call gettyimeofday error\n");
    }
    return ((double) (tp.tv_sec)) +
           (((double) tp.tv_usec) / 1000000.0);
#endif
}

void MeanTimeStatStart(MEANTIMESTAT * pStat)
{
    pStat->st = time_so_far();
}

void MeanTimeStatEnd(MEANTIMESTAT * pStat, int seconds, const char * msg)
{
    time_t curr_time;

    pStat->allt += time_so_far() - pStat->st;
    pStat->cnt ++;
    time(&curr_time);

    if (curr_time - pStat->start_time >=seconds)
    {
        printf("%s : %f ms\n", msg, pStat->allt*1000/pStat->cnt);
        pStat->allt = 0;
        pStat->cnt = 0;
    }
}

off_t GetArgValue(char * szValue)
{
    int iLen=strlen(szValue);
    off_t lValue;
    int i;

    lValue=0;
    for(i=0;i<iLen-1;i++)
    {
        if(szValue[i]<'0' || szValue[i]>'9')
        {
            return -1;
        }
        lValue=lValue*10+szValue[i] - '0';
    }

    if(szValue[i]>='0' && szValue[i]<='9')
    {
        lValue=lValue*10+ szValue[i] - '0';
    }
    else if(szValue[i]=='k' ||    szValue[i]=='K')
    {
        lValue=lValue*1024;
    }
    else if(szValue[i]=='m' ||    szValue[i]=='M')
    {
        lValue=lValue*1024*1024;
    }
    else if(szValue[i]=='g' ||    szValue[i]=='G')
    {
        lValue=lValue*1024*1024*1024;
    }
    else
    {
        return -1;
    }
    return lValue;
}


/* 一直读取指定的字节的内容后退出 */
int ReadData(int fd, char * p, int len)
{
    int n=0;
    int readnums;

    while (n<len)
    {
        readnums = read(fd, &p[n], len -n);
        if (readnums < 0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            else
            {
                return -1;
            }
        }
        else if (readnums == 0) /*读取数据结束*/
        {
            return n;
        }
        n+=readnums;
    }
    return n;
}

RATESTAT * AllocRateStat()
{
    RATESTAT * pStat = (RATESTAT *)malloc(sizeof(RATESTAT));
    pthread_mutexattr_init(&pStat->attr);
    pthread_mutexattr_settype(&pStat->attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&pStat->mutex, &pStat->attr);
    pStat->st = time_so_far();
    pStat->value = 0;
    return pStat;
}

void FreeRateStat(RATESTAT * pStat)
{
    pthread_mutex_destroy(&pStat->mutex);
    free(pStat);
}

void RateStatAddValue(RATESTAT * pStat, int64_t value)
{
    pthread_mutex_lock(&pStat->mutex);
    pStat->value += value;
    pthread_mutex_unlock(&pStat->mutex);
}

void GetRateStat(RATESTAT * pStat, double * pSec, int64_t * pValue)
{
    double st;
    double et;

    pthread_mutex_lock(&pStat->mutex);
    st = pStat->st ;
    *pValue = pStat->value;
    pthread_mutex_unlock(&pStat->mutex);
    et = time_so_far();
    *pSec = et - st;
}

void GetRateStatAndReset(RATESTAT * pStat, double * pSec, int64_t * pValue)
{
    double st;
    double et;

    pthread_mutex_lock(&pStat->mutex);
    st = pStat->st ;
    *pValue = pStat->value;
    et = time_so_far();
    *pSec = et - st;
    pStat->st = et;
    pStat->value = 0;
    pthread_mutex_unlock(&pStat->mutex);
}

void RateStatReset(RATESTAT * pStat)
{
    pthread_mutex_lock(&pStat->mutex);
    pStat->st = time_so_far();
    pStat->value = 0;
    pthread_mutex_unlock(&pStat->mutex);
}


void PrtLog(int iLogLevel,const char * format,...)
{
    va_list arg_ptr;
    time_t now;
    struct tm tmCurr;


    if ( !(iLogLevel & g_loglevel) )
    {
        return;
    }

    time(&now);
    localtime_r(&now,&tmCurr);

    printf("%04d-%02d-%02d %02d:%02d:%02d : ",
            tmCurr.tm_year+1900,
            tmCurr.tm_mon+1,
            tmCurr.tm_mday,
            tmCurr.tm_hour,
            tmCurr.tm_min,
            tmCurr.tm_sec
           );

    switch(iLogLevel)
    {
    case ERR_LOG:
        printf("ERROR : ");
        break;
    case INFO_LOG:
        printf("INFO  : ");
        break;
    case STAT_LOG:
        printf("STAT  : ");
        break;
    case DEBUG0_LOG:
        printf("DEBUG0: ");
        break;
    case DEBUG1_LOG:
        printf("DEBUG1: ");
        break;
    default:
        printf("UNKNOW: ");
    }

    va_start(arg_ptr, format);
    vprintf(format, arg_ptr);
    va_end(arg_ptr);
    fflush(stdout);
    fdatasync(STDOUT_FILENO);
}


int OptimizedSocket(int sockfd, int sock_buf_size)
{
    struct linger li;
    int flag;
    char szErrMsg[512];
    int iRet;

    li.l_onoff = 1;
    li.l_linger = 10;
    iRet = setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof(li));
    if (iRet != 0)
    {
        GetSysErrorInfo(errno, szErrMsg);
        PrtLog(ERR_LOG,"call setsockopt(sockfd, SOL_SOCKET, SO_LINGER,...) : %s", szErrMsg);
    }

    iRet = setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (const char*)&sock_buf_size, sizeof(int));
    if (iRet != 0)
    {
        GetSysErrorInfo(errno, szErrMsg);
        PrtLog(ERR_LOG,"setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF...) : %s", szErrMsg);
    }

    iRet = setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (const char*)&sock_buf_size, sizeof(int));
    if (iRet != 0)
    {
        GetSysErrorInfo(errno, szErrMsg);
        PrtLog(ERR_LOG,"setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF...) : %s", szErrMsg);
    }

    flag = 1;
    iRet = setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag) );
    if (iRet != 0)
    {
        GetSysErrorInfo(errno, szErrMsg);
        PrtLog(ERR_LOG,"setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY...) : %s", szErrMsg);
    }

    set_nonblock(sockfd);

    return 0;
}

int CloseSocketImmediate(int sockfd)
{
    int iRet;
    struct linger li;
    li.l_onoff = 0;
    li.l_linger = 0;
    iRet = setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (char *) &li, sizeof(li));
    close(sockfd);
    return 0;
}

int GetBinPath(char * argv0,char * szPath)
{
    szPath[0] = 0;
    if (strstr(argv0,"/")==NULL)
    {
        sprintf(szPath,".");
    }

    strcpy(szPath, argv0);

	int i;
	int len;
	len = strlen(szPath);
	for (i=len-1; i>=0; i--)
	{
	    if(szPath[i]=='/')
		{
		    szPath[i]='\0';
			return 0;
		}
	}
	if (i<0)
	{
	    szPath[0]='\0';
	}
    return 0;
}

/* 检查程序是否运行了,返回0表示不存在，返回1表示存在，返回-1，表示错误 */
int CheckIsRunning(char * pidfile)
{
    if (access(pidfile, F_OK)!=0) /*文件不存在说明程序没有运行*/
    {
        return 0;
    }

    int fd;
    int res;
    char szPid[128];
    int pid;
    char szErrInfo[512];

    fd = open(pidfile, O_RDONLY);
    res = read(fd, szPid, 120);
    if(res <0)
    {
        GetSysErrorInfo(errno, szErrInfo);
        close(fd);
        PrtLog(ERR_LOG, "Can not read file %s : %s", pidfile, szErrInfo);
        return -1;
    }
    close(fd);

    pid = atoi(szPid);
    if (pid<=0)
    {
        return 0;
    }
    res = kill(pid, 0);
    if (res == 0)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}


int CreatePidFile(char * pidfile)
{
    int res;
    int fd;
    char szPid[128];
    int len;
    char szErrInfo[512];

    if (access(pidfile,F_OK) == 0) /*旧pid文件存在*/
    {
        res = unlink(pidfile);
        if (res != 0)
        {
            GetSysErrorInfo(errno, szErrInfo);
            PrtLog(ERR_LOG,
                   "\ncan not delete old pid file %s:%s\n", pidfile, szErrInfo);
            return -1;
        }
    }

    fd = open(pidfile,O_WRONLY|O_CREAT,S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd <0)
    {
        GetSysErrorInfo(errno, szErrInfo);
        PrtLog(ERR_LOG,
                "\ncan not create pid file %s:%s\n", pidfile, szErrInfo);

        return -1;
    }

    len = sprintf(szPid,"%d", (int)getpid());
    res = write(fd,szPid,len);
    if (res <=0)
    {
        GetSysErrorInfo(errno, szErrInfo);
        PrtLog(ERR_LOG,
                "\ncan not write pid file %s:%s\n", pidfile, szErrInfo);

        close(fd);
        return -1;
    }
    close(fd);
    return 0;
}
