import ftplib
import os
import sys
import re
import time
import datetime
import pprint
import fnmatch 
hostname = "ftp2.census.gov"
username = "anonymous"
password = "ftp"
gEnableTransfer = True
g_cycle_delay = 60 * 10
gVerbose = False
gDeleteRemote = False
today = datetime.date.today() # "date"
# https://joescam1.dlinkddns.com:8002/cgi-bin/sddownload.cgi?file=/video/20170917/17/Front_Timer20170917_174500.jpg
# https://joescam1.dlinkddns.com:8003/cgi-bin/sddownload.cgi?file=/picture/20151129/09/Snapshot_2_20151129_090028_2872.jpg
#https://joescam1.dlinkddns.com:8003/cgi-bin/sddownload.cgi?file=/video/20170921/06/RECORD20170921_064602.jpg

class Error(Exception): pass

# Read remote directory contents
def getDir(ftp):
	data = []
	ftp.dir(data.append)
	return data

class FtpBase(object):
    def __init__(self, match):
        groups = match.groupdict()
        if 'type' in groups: # Unix style
            self.date = match.group('month') + " " + match.group('day')
            year = match.group('year')
            if year: # Unix style
                self.date +=  " " + year
                t = datetime.datetime.strptime(self.date, "%b %d %Y") # "time.struct_time"
            else:
                self.time = match.group('time')
                timestamp = self.date + " " + self.time
                t = datetime.datetime.strptime(timestamp, "%b %d %H:%M")
                t = datetime.datetime(year=today.year, month=t.month, day=t.day, hour=t.hour, minute=t.minute)
            self.name = match.group('filename')
            self.timestamp = "<none>"
            self.mtime = t.timetuple() # "time.struct_time"
            #print("TIME : ", self.mtime, " == ", self.date)
        else:
            self.date = match.group('date')
            self.time = match.group('time')
            self.name = match.group('filename')
            timestamp = self.date + " " + self.time
            t = time.strptime(timestamp, "%m-%d-%y %I:%M%p")
            self.mtime = t # "time.struct_time"
        #print("TIME (%s) == (%s)" % (timestamp, str(t)))

class FtpFile(FtpBase):
    def __init__(self, match):
        FtpBase.__init__(self, match)
        self.size = int(match.group('size'))

class FtpDir(FtpBase):
    def __init__(self, match):
        FtpBase.__init__(self, match)
        self.size = 0
        #print("DIR(%s, %s, %s)" %(self.date, self.time, self.name))

def include_file(name):
    #if exclude_file(name):
    #    return False
    return True
    #return fnmatch.fnmatch(name, "*.csv") or fnmatch.fnmatch(name, "*.txt")

def exclude_file(name):
    return fnmatch.fnmatch(name, "*.zip") or fnmatch.fnmatch(name, "*.xls")

def exclude_folder(name):
    return False
    #if name == "cities":
    #    return False
    #if name == "totals":
    #    return False
    #d = name[0:1]
    #if (d >= '0') and (d <= '9'):
    #    return False
    #return True
    #if name == "1900-1980":
    #    return True
    #if name == "1980-1990":
    #    return True
    #if name == "2010-2015":
    #    return True
    #if name == "2010-2016":
    #    return True
    #if name == "2010-2017":
    #    return True
    #if name == "2010-2018":
    #    return True
    #if name.lower() == 'aspnet_client':
    #    return True
    return False

def parseDirs(data):
    #04-12-17  03:02AM       <DIR>          aspnet_client
    #09-02-17  03:44PM             15706404 DCS-2330L_REVA_FIRMWARE_v1.14.03_BETA.zip
    #09-02-17  03:37PM       <DIR>          deck
    #09-02-17  09:48PM       <DIR>          outside
    #09-02-17  10:31PM       <DIR>          upload
    #drwxrwsr-x    4 844      i-dis        4096 Aug 24  2016 counties
    #lrwxrwxrwx    1 844      i-admin        17 Jan 24  2014 AOA -> /acstabs/docs/AOA
    #rwx    count?    username    group   size Mon Day Year/Time filename -> linkedname
    parse_dos =  re.compile(R'(?P<date>[0-9][0-9]-[0-9][0-9]-[0-9][0-9]) *(?P<time>[0-9][0-9]:[0-9][0-9][AP]M) *(?P<size><DIR>|[0-9]+) *(?P<filename>.*)')
    # (?P<*>)
    parse_unix = re.compile(R'^(?P<type>.)(?P<mode>[^ ]+) *([0-9]*) *(?P<owner>[^ ]+) *(?P<group>[^ ]+) *(?P<size>[0-9]+) +(?P<month>[a-zA-Z]{3,3}) +(?P<day>[0-9]{1,2}) *(?:(?P<year>[0-9]{4,4})|(?P<time>[0-9][0-9]:[0-9][0-9])) *(?P<filename>.*)(?: +-> +(?P<link>.*))??')
    dirs = []
    files = [] 
    if gVerbose:
        print("parseDirs", file=sys.stderr)
    for line in data:
        line = line.strip()
        if gVerbose:
            print("parseDirs", line, file=sys.stderr)
        i = line.find("<DIR>")
        m = parse_unix.match(line)
        if not m:
            print("Failed to match", line, file=sys.stderr)
        else:
            #if gVerbose:
            #    for g  in m.groupdict():
            #        print("Group ", g, m.group(g))
            #        #print(g, m.group(g))
            #    print("")

            groups = m.groupdict()
            if 'type' in groups: # Unix style
                t = m.group('type')
                if t == 'd': # dir
                    dirs.append(FtpDir(m))
                if t == '-': # file
                    files.append(FtpFile(m))
                if t == 'l': # link
                    #files.append(FtpFile(m))
                    pass
            else:
                if m.group('size') == "<DIR>":
                    dirs.append(FtpDir(m))
                else:
                    files.append(FtpFile(m))
    return (dirs, files)

def getFile(ftp, ftp_file, newRemote, newLocal):
    print("GET (size:%12.3fKB, %s  start at %s)" % (ftp_file.size / 1024.0, newLocal, str(datetime.datetime.today())), file=sys.stderr)
    try:
        ok = False
        start_time = time.time()
        with open(newLocal, "wb") as fd:
            ftp.retrbinary("RETR " + ftp_file.name, fd.write)
        ok = True
        end_time = time.time()
        elapsed = end_time-start_time

        print("    %12.3fKB, %8.2fs, %10.3fKBps" % (ftp_file.size / 1024.0, elapsed ,(ftp_file.size / elapsed) / 1024.0), file=sys.stderr)
        if gDeleteRemote:
            delFile(ftp, ftp_file, newRemote, newLocal)
    except ftplib.Error as e:
        print("Error during transfer", newRemote, e, file=sys.stderr)
    except OSError as e:
        print("Error during transfer", newRemote, e, file=sys.stderr)
    finally:
        if not ok:
            print("Failed to transfer %s as local file %s" % (newRemote, newLocal), file=sys.stderr)
            if os.path.exists(newLocal):
                os.remove(newLocal)

def delFile(ftp, ftp_file, newRemote, newLocal):
    try:
        #print("Delete",ftp_file.name)
        ftp.delete(ftp_file.name)
    except ftplib.Error as e:
        print("Error deleting remote file", newRemote, e, file=sys.stderr)
    except OSError as e:
        print("Error deleting remote file", newRemote, e, file=sys.stderr)

def processFiles(ftp, root, local, files, limit = 0):
    for ftp_file in files:
        newLocal = os.path.join(local, ftp_file.name)
        newRemote = root + "/" + ftp_file.name
        transfer = True
        if include_file(ftp_file.name):
            if os.path.exists(newLocal):
                statinfo = os.stat(newLocal)
                if ftp_file.size != statinfo.st_size:
                    print("Size mismatch (remote:%d, local:%d" % (ftp_file.size, statinfo.st_size), file=sys.stderr)
                else:
                    secs = time.mktime(ftp_file.mtime)
                    if statinfo.st_mtime != secs:
                        print("Time mismatch(%d, %d) diff=%d" % (statinfo.st_mtime, secs, statinfo.st_mtime - secs), file=sys.stderr)
                    else:
                        #print("SKIP FILE", ftp_file.name, file=sys.stderr)
                        transfer = False
                        if gDeleteRemote:
                            delFile(ftp, ftp_file, newRemote, newLocal)

            if transfer and (limit == 0 or ftp_file.size >= limit):
                print("SKIP FILE LARGE", ftp_file.name, ", size=", ftp_file.size, file=sys.stderr)
                transfer = False
            # Transfer the file
            if gEnableTransfer:
                if transfer:
                    getFile(ftp, ftp_file, newRemote, newLocal)

                    if os.path.exists(newLocal):
                        secs = time.mktime(ftp_file.mtime)
                        os.utime(newLocal, (secs, secs))
        else:
            pass
    pass

def processDirs(ftp, root, local, dirs, recurse):
    # Now recurse to subdirectories
    for ftp_dir in dirs:
        #print("FTP_DIR",ftp_dir.name)
        newLocal = os.path.join(local, ftp_dir.name)

        if not exclude_folder(ftp_dir.name):
            if recurse:
                recurse(ftp_dir.name)
            if ftp_dir.mtime != 0:
                secs = time.mktime(ftp_dir.mtime)
                os.utime(newLocal, (secs, secs))
    pass

def processFiles2(ftp, root, local, files, limit = 0):
    total = 0
    for ftp_file in files:
        total += ftp_file.size
        tstr = time.strftime("%Y/%m/%d %H:%M:%S", ftp_file.mtime)
        fullPath = root + "/" + ftp_file.name
        print("%14d %s %s" % (ftp_file.size, tstr, fullPath))

#   print("Total size", total)

def processDirs2(ftp, root, local, dirs, recurse):
    for ftp_dir in dirs:
        tstr = time.strftime("%Y/%m/%d %H:%M:%S", ftp_dir.mtime)
        fullPath = root + "/" + ftp_dir.name
        print("%14d %s %s" % (0, tstr, fullPath))
        #print("DIR : ", ftp_dir.name, ftp_dir.size, tstr)

    for ftp_dir in dirs:
        if not exclude_folder(ftp_dir.name):
            if recurse:
                recurse(ftp_dir.name)
    pass

def getAllFiles(ftp, root, local, processFiles, processDirs, limit = 0):
    if gVerbose:
        print("getAllFiles(%s, %s" %(root, local), file=sys.stderr)
    if not os.path.exists(local):
        os.mkdir(local)

    try:
        ftp.cwd(root)
    except ftplib.error_perm as e:
        print("Cannot find remote directory ", root, file=sys.stderr)
        return

    try:
        data = getDir(ftp)
    except ftplib.error_perm as e:
        print("Cannot iterate remote directory ", root, file=sys.stderr)
        return

    if gVerbose:
        print("getDir returned ", len(data))
    (dirs, files) = parseDirs(data)

    #print("Transferring files")
    processFiles(ftp, root, local, files, limit=limit)

    processDirs(ftp, root, local, dirs, lambda dirName : getAllFiles(ftp, root + "/" + dirName, os.path.join(local, dirName), processFiles, processDirs, limit=limit))

def transferFiles():
    try:
        if gVerbose:
            print("Opening", hostname)
        ftp = ftplib.FTP(hostname, username, password)
        if gVerbose:
            print("Opened", hostname)
    except OSError as e:
        print("Error opening connection to", hostname, ":", e)
        raise Error(e)

    #getAllFiles(ftp, "/programs-surveys/popest/tables/1980-1990/counties/totals/", "local", processFiles, processDirs)
    #getAllFiles(ftp, "/programs-surveys/popest/tables", "E:/datascience/programs-surveys/popest/tables/2000-2010", processFiles, processDirs)
    #getAllFiles(ftp, "/programs-surveys/popest/tables", "E:/datascience/programs-surveys/popest/tables", processFiles2, processDirs2)

    # Get all technical data files
    #getAllFiles(ftp, "/programs-surveys/popest/technical-documentation", "E:/datascience/programs-surveys/popest/technical-documentation/", processFiles, processDirs)

    # Get pop estimates for 2000-2010
    #getAllFiles(ftp, "/programs-surveys/popest/tables/2000-2010/intercensal/county", "E:/datascience/programs-surveys/popest/tables/2000-2010/intercensal/county", processFiles, processDirs)
    #getAllFiles(ftp, "/programs-surveys/popest/tables/", "E:/datascience/programs-surveys/popest/tables/", processFiles, processDirs)

    #getAllFiles(ftp, "/programs-surveys/popest/datasets", "E:/datascience/programs-surveys/popest/datasets", processFiles2, processDirs2)
    getAllFiles(ftp, "/programs-surveys/popest/tables/", "E:/datascience/programs-surveys/popest/tables/", processFiles, processDirs, limit=100000)
    print("GETTING LARGER")
    getAllFiles(ftp, "/programs-surveys/popest/tables/", "E:/datascience/programs-surveys/popest/tables/", processFiles, processDirs, limit=1000000)
    print("GETTING LARGER")
    getAllFiles(ftp, "/programs-surveys/popest/tables/", "E:/datascience/programs-surveys/popest/tables/", processFiles, processDirs, limit=10000000)

    return True


def test1():
    with open("sample.txt", "r") as data:
        (dirs, files) = parseDirs(data)

    processFiles2(0, 0, 0, files)
    processDirs2(0, 0, 0, dirs, None)
    #for ftp_file in files:
    #    print("FTP_FILE",ftp_file.name)
    #for ftp_dir in dirs:
    #    print("FTP_DIR",ftp_dir.name)

def testfn(fn):
    print("TESTFN")
    fn()

def caller(fn, limit):
    if limit > 0:
        fn(lambda : caller(fn, limit-1))

def main():
    #caller(testfn,3)
    #test1()
    transferFiles()

main()

