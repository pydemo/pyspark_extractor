"""

set PYSPARK_EXTRACTOR_LINUX_HOST=home.root.net
set PYSPARK_EXTRACTOR_LINUX_PWD=***
python extract.py -q query.sql -o test.csv -d 20200604 


"""
import os, sys, time, getpass
from os.path import isfile, basename
import paramiko
from pprint import pprint as pp
e=sys.exit
 
 
def timer(argument):
    def decorator(function):
        def wrapper(*args, **kwargs):
            start = time.time()
            print("---> Entering %s:%s." % (argument, function.__name__))
            result = function(*args, **kwargs)
            end = time.time()
            print("<--- Exiting %s:%s:  [ %0.3f ] sec." % (argument, function.__name__, (end - start)))
            return result
        return wrapper
    return decorator
    
import click
click.disable_unicode_literals_warning = True

e=sys.exit

if 1:
    LINUX_HOST=os.environ.get('PYSPARK_EXTRACTOR_LINUX_HOST')
    assert LINUX_HOST, 'set PYSPARK_EXTRACTOR_LINUX_HOST=bdnjr004x03h5.nam.nsroot.net'
    LINUX_PWD=os.environ.get('PYSPARK_EXTRACTOR_LINUX_PWD')
    assert LINUX_PWD, 'set PYSPARK_EXTRACTOR_LINUX_PWD=***'


LINUX_USER=getpass.getuser()
NBYTES = 1024*16

SPARK = 'spark'
MR 	  = 'mr'

VERBOSE = True
HIVE_ENGINE = SPARK

DONE_FLAG = 'Done.'
DONE = ["print('%s')" % DONE_FLAG]
@timer (basename(__file__))
def put_file(sftp, local_file, remote_file, mode=None):

    print(local_file, remote_file)
    assert isfile(local_file), 'Source file "%s" is missing.' % local_file
    try:
        sftp.unlink(remote_file)
        print('Remote file deleted')
    except FileNotFoundError as not_found:
        
        print ('Ignoring remote FileNotFoundError.')
    #e(0)
    
    try:
        print('start transport...')
        sftp.put(local_file, remote_file)
    except :
        
        raise
    rstat=sftp.stat(remote_file)
    
    lstat=os.stat(local_file)
    print(lstat.st_size, rstat.st_size)
    assert lstat.st_size == rstat.st_size, "File size mismatch (%d<>%d)" % (lstat.st_size, rstat.st_size)
    if mode:
        sftp.chmod(remote_file, mode)

@timer (basename(__file__))
def connect():
    global ssh
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(LINUX_HOST, username=LINUX_USER, password=LINUX_PWD)



@timer (basename(__file__))
def handshake():
    global conn, ssh, stdout, stop_flag, queue
    conn = ssh.invoke_shell()
    if 1: 
        stdout = conn.makefile('r') 
    if 1:
        queue = Queue()
        stop_flag = threading.Event()
        stop_flag.set()
        t = output_thread(stop_flag, stdout, queue)
        t.daemon = True

        
        t.start()        
    print("Interactive SSH session established")

    output = conn.recv(NBYTES)
    conn.send("kinit\n")
    time.sleep(0.5)
    conn.send(LINUX_PWD +'\n')

    output = conn.recv(NBYTES)
    payload =[
    'setterm -linewrap off',
    #'tput rmam',
    'pyspark2']
    for cmd in payload:
        conn.send(cmd +'\n')
    #i=0
    
    stderr_data=[]
    time.sleep(0.5)
    #//'0: jdbc:hive2://icgbigdata-nj.nam.nsroot.net:>'
    bee_prompt = "SparkSession available as 'spark'."
    
    start = time.time()
    start_show = time.time()
    got_bee_prompt=False
    while True:

        if conn.recv_ready():
            data = conn.recv(NBYTES)
            
            for line in data.decode("ascii").splitlines():
                end = time.time()
                print ('[ %0.2f ][ %0.3f ]||| %s' %((end - start_show),(end - start),line))
                line = line.strip()
                if line ==bee_prompt:
                    print('GOT PROMPT:', bee_prompt)
                    got_bee_prompt = True
                    break
            start = time.time()
        if conn.recv_stderr_ready():
            stderr_data.append(conn.recv_stderr(NBYTES))
        if conn.exit_status_ready():
            break
        if got_bee_prompt:
            break

    if stderr_data:
        print('*'*80);
        print('*'*80);
        pp(stderr_data)
        print('*'*80);
        print('*'*80);
@timer (basename(__file__))
def show(exit_on=DONE_FLAG):
    global conn, start_job
    
    ecnt=1
    py_prompt = '>>>'
    got_py_prompt = False
    got_exit_str = False
    stderr_data=[]
    i=0
    start = time.time()
    start_show = time.time()
    while True:
        
        if conn.recv_ready():
            
            data = conn.recv(NBYTES)
            for i, line in enumerate(data.decode("ascii").splitlines()):
                line=line.strip()
                #print('SHOW:', line)
                if line ==py_prompt:
                    
                    got_py_prompt = True
                    break
                if line ==exit_on:
                    
                    got_exit_str = True
                    break
                if line:
                    end = time.time()
                    print ('[ %0.1f ][ %0.2f ][ %0.3f ][%s]: %s' % ((end - start_job),(end - start_show),(end - start), ecnt, line))
            start = time.time()
        if conn.recv_stderr_ready():
            stderr_data.append(conn.recv_stderr(NBYTES))
        if conn.exit_status_ready():
            break

        if got_py_prompt:
            print('GOT PROMPT')
            got_py_prompt=False
            ecnt +=1
            start = time.time()
        if got_exit_str:
            break

    if stderr_data:
        print('*'*80);
        print('*'*80);
        for line in stderr_data:
            print ('ERROR: ', line)
        print('*'*80);
        print('*'*80);

def get_line(q):
    line=None
    try:  
        line = q.get(timeout=.2) # q.get_nowait(.1) # or q.get(timeout=.1)
        line=line.lstrip('...').lstrip('>>>').strip() #.replace('\r','')
        rpos=line.find('\r')
        if rpos>0:
            #print(555, rpos)
            new_line=line[:rpos]+line[rpos+2:]
            line=new_line                
    except Empty:
        #print('no output yet')
        #print('CMD:')
        #pp(cmd)
        line='Empty'
        time.sleep(1)
        #raise
    assert line
    return line
if 1:
    import sys
    from subprocess import PIPE, Popen
    from threading  import Thread
    import threading

    try:
        from queue import Queue, Empty
    except ImportError:
        from Queue import Queue, Empty  # python 2.x

    ON_POSIX = 'posix' in sys.builtin_module_names

    def enqueue_output1(out, queue):
        data=[]
        try:
            for line in iter(out.readline, b''):
                if line:
                    queue.put(line)
                    #data.append(line)
            #queue.put(data)
            out.close()
        except OSError as er:
            
            if str(er) != 'File is closed':
                raise

    class output_thread(threading.Thread):
        def __init__(self, event, stdout, queue):
            threading.Thread.__init__(self)
            self.stopped = event
            self.stdout = stdout
            self.queue  = queue
            self.name='output_thread'
        @timer (basename(__file__))
        def run(self):
            i=1
            while True:
                if not self.stopped.isSet():
                    try:
                        line= next(iter(self.stdout.readline, b''))
                        if line:
                            self.queue.put(line)
                            i += 1
                            if not i%10:
                                print(self.name, i)
                    except OSError as er:
                        print(i, str(er))
                        time.sleep(.2)
                        if str(er) != 'File is closed':
                            raise
            #print(self.name, i,'CLOSING STDOUT')
            #self.stdout.close()
    

@timer (basename(__file__))
def show2(exit_on=DONE_FLAG):
    global conn
    AnalysisException = 'pyspark.sql.utils.AnalysisException:'
    ParseException= 'pyspark.sql.utils.ParseException:' 
    stderr_data=[]
    i=0
     
    ecnt=1
    stderr_data=[]
    pyderr_data=[]
    py_prompt = '>>>'
    got_py_prompt = False        
    #exit_on='JobDone'
    got_exit_str=False
    start = time.time()
    start_show = time.time()        
    while True:
        time.sleep(0.1)
       # print('$$$')
        if conn.recv_ready():
            data = conn.recv(NBYTES)
            for i, line in enumerate(data.decode("ascii").splitlines()):
                line=line.strip()
                #print('$$$$$$$$$$$$$$$$$', line)
                                    
                if line ==py_prompt:
                    
                    got_py_prompt = True
                    break
                if line ==exit_on:
                    
                    got_exit_str = True
                    break                        
                if AnalysisException in line:
                    print('GOT ERROR (AnalysisException)')
                    pyderr_data.append(line)
                    break
                if ParseException in line:
                    print('GOT ERROR (ParseException)')
                    pyderr_data.append(line)
                    break 
                if line:
                    end = time.time()
                    print ('[ %0.1f ][ %0.2f ][ %0.3f ][%s]: %s' % ((end - start_job),(end - start_show),(end - start), ecnt, line))
            start = time.time()                        
        if conn.recv_stderr_ready():
            stderr_data.append(conn.recv_stderr(NBYTES))
        if conn.exit_status_ready():
            break

        if got_py_prompt:
            print('GOT PROMPT', ecnt)
            got_py_prompt=False
            ecnt +=1
            start = time.time()
        if got_exit_str:
            break
        if pyderr_data:
            break

    if stderr_data:
        print('*'*80);
        print('*'*80);
        for line in stderr_data:
            print ('STDERR: ', line)
        print('*'*80);
        print('*'*80);
        e()
    if pyderr_data:
        print('*'*80);
        print('*'*80);
        for line in pyderr_data:
            print ('PYERROR: ', line[:200])
        print('*'*80);
        print('*'*80);
        e()
@timer (basename(__file__))
def match_output(payload):
    global conn

        #asyncore.loop(timeout=0.1)
    if_mismatch = False
    stop_flag.clear()
    i=0
    for pld in payload:
        prev_cmd=None
        cmds= pld.split('\n')
        for cid,cmd in enumerate(cmds):
            if cmd:
                conn.send(cmd +'\n')
                line = get_line(queue)
                #print(i,'MATCH: ', line)
                if 1:
                    while line=='Empty':
                        
                        line = get_line(queue)
                        print('SQL PARSE [Empty]', i); i+=1
                cmd=cmd.strip()
                #print(i,'CMD: ', cmd)
                if not line==cmd:
                    print(cid,'LINE:')
                    pp(line)
                    print(cid,'CMD:')
                    pp(cmd)
                    print(cid,'PREV CMD:')
                    pp(prev_cmd)               
                    if cid == 1 and prev_cmd==line:
                        line = get_line(queue)
                        print('ALIGNED LINE:')
                        pp(line)
                    else:
                        stop_flag.set()
                        if_mismatch = True
                        break
                prev_cmd = cmd

    stop_flag.set()
    #t.join()
    #stdout.close()
    #del stdout
    if if_mismatch:
        raise Exception('Output mismaich')
    
def send_pld(payload):
    global conn
    for pld in payload:
        #print(pld)
        for line in pld.split('\n'):
            conn.send(line +'\n')
@timer (basename(__file__))
def execute(query_file, trade_date):
    global conn, start_job, stdin, stdout
    q=None
    start_job = time.time()
    with open(query_file, 'r') as fh:
        q=fh.read()
    assert q
    
    q= q.replace('${TRADE_DATE}',trade_date)
    q= q.strip().strip(';')
    q= q.replace('\t','    ')
    q= q.replace('\r\n','\n')
    payload= [
    'spark.conf.set("spark.debug.maxToStringFields", 1000)',
    'df=spark.sql("""%s""")' % (q)] +DONE 
    

    if 0:match_output(payload); show2()
    if 1:
        send_pld(payload)
        show2()

    


    #e()
    print(123)
    if 1:
        payload= ['df.explain',
        #'data=df.collect()',
        #'print(len(data))',
        'cols=list(df.columns)',
        'cols'
        ] + DONE
        
        if 1:
            send_pld(payload)
        show2()
        
    payload=["""
print('DATA:'+'|'.join(cols))
for rid, row in enumerate(df.collect()): 
    out=[]
    for x in cols: 
        out.append(str(getattr(row,x)))
    
    print('DATA:'+'|'.join(out))
"""] + DONE
    
    payload=["""
from collections import OrderedDict
import csv
def extract(df, out_file, limit=0, show=False):
    cols=list(df.columns)
    rcnt=0
    with open(out_file, 'w') as f:  
        for row in df.collect():
            #print(row)
            
            out = OrderedDict()
            for x in cols: out[x] = getattr(row,x)
    
            if limit and rcnt> limit:
                
                break;
            if not rcnt:
                w = csv.DictWriter(f, cols, delimiter ='|')
                w.writeheader()
                
            if 1:
                if show: print(out.values())
                w.writerow(out)
                rcnt +=1
    if rcnt:
        print('Extracted [%d] to %s' % (rcnt,out_file))
    else:
        print('No data to extract.')
""", "extract(df=df, out_file='CGMI_AMC_ALL.csv', limit=0, show=False)"] + DONE
    if 0:
        match_output(payload)
        show2()
    if 1:
        send_pld(payload)
        show2()


@timer (basename(__file__))
def dump_data(out_fn,exit_on=DONE_FLAG):
    global conn, start_job
    
    ecnt=1
    py_prompt = '>>>'
    got_py_prompt = False
    got_exit_str = False
    stderr_data=[]
    i=0
    start = time.time()
    start_show = time.time()
    data_line='DATA:'
    rcnt = 0
    header=True
    with open(out_fn, 'a') as fh:
        while True:
            
            if conn.recv_ready():
                
                data = conn.recv(NBYTES)
                for i, line in enumerate(data.decode("ascii").splitlines()):
                    line=line.strip()
                    end = time.time()
                    
                    if line ==py_prompt:
                        
                        got_py_prompt = True
                        break
                    if line ==exit_on:
                        
                        got_exit_str = True
                        break
                    if line.startswith(data_line):
                        line=line.lstrip(data_line)+'\n'
                        fh.write(line)
                        rcnt +=1
                        if not rcnt%1000:
                            print ('EXTRACT: Dumped: %d records' % rcnt)
                    elif line:
                        end = time.time()
                        print ('[ %0.1f ][ %0.2f ][ %0.3f ][%s]: %s' % ((end - start_job),(end - start_show),(end - start), ecnt, line))
                start = time.time()
            if conn.recv_stderr_ready():
                stderr_data.append(conn.recv_stderr(NBYTES))
            if conn.exit_status_ready():
                break

            if got_py_prompt:
                print('GOT PROMPT')
                got_py_prompt=False
                ecnt +=1
                start = time.time()
            if got_exit_str:
                break

    if stderr_data:
        print('*'*80);
        print('*'*80);
        for line in stderr_data:
            print ('ERROR: ', line)
        print('*'*80);
        print('*'*80);
    print ('EXTRACT: Dumped: %d records' % (rcnt-1)+ ' and header' if header else '')

@click.command()
@click.option('-d', '--trade_date',default = None,	help = 'Trade_date.', 		required=True )
@click.option('-l', '--lame_duck', default = 0,		help = 'Limit', type=int, 	required=False )
@click.option('-q', '--query_file',default = None,	help = 'Input query file', 	required=True )
@click.option('-o', '--out_file',default = None,	help = 'Output CSV file', 	required=True )

@timer (basename(__file__))
def main(**kwargs):
    limit 	   = kwargs.get('lame_duck')
    trade_date = kwargs.get('trade_date')
    query_file = kwargs.get('query_file')
    out_file = kwargs.get('out_file')
    if 1:
        
        #query_file= 'query.sql'
        assert  isfile(query_file), 'Query file does not exists.'
        lstat=os.stat(query_file)
        assert lstat.st_size>0, "Query file is empty"

        
        if isfile(out_file): 
            os.remove(out_file)
        
    connect()
    handshake()
    execute(query_file, trade_date)
    if 0:
        dump_data(out_file)
        
if __name__ == "__main__":
    
    main()

