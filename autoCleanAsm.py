#! /usr/bin/env python

#Program to automate cleaning and assembly of raw sequence data in:
#Microbiome-1 in /gpfs/pace1/project/bio-stewart-repository/SequencedDatasets

#Clean and Assemble all files in server directory, all files in current directory, or single file
#^ Should be user input

#run script on server in ~/data/scripts

#try for single file first, can use grep and shell script to import all files names ot run
#may need to have shell script call python script then do qsubs

Usage = '''
Automates cleaning and assembly of raw sequence data

Usage: autoCleanAsm [filename] : works on single file in repository
	autoCleanAsm [filenames] : works on .fq.gz files in repository
	autoCleanAsm -a : works on every file in repository
	autoCleanAsm -c [directory]	: works on every file in directory
	autoCleanAsm -l [filenames/-c] : works on local fasta files in current directory (~/data/scripts)
	autoCleanAsm -o [filenames/-a/-c] : Only cleans and assembles
	autoCleanAsm -n [filenames/-a/-c] : uses BBNorm to normalize cleaned sequence before assembling
	autoCleanAsm -k [filenames/-a/-c] : modifies K values
	
	replace ### with '' to debug - uncomments print statements'''
	
import sys
import os
import re
import shutil
import zipfile
import gzip
import tarfile
import subprocess
import shlex

allFiles = False
currentFiles = False
local = False
assembleOnly = False
norm = False
k = False
fileList = []
myfile = ''

if len(sys.argv) < 2:
	sys.stderr.write('argument error')
	print Usage
	quit()
	
if '-n' in sys.argv:
	if len(sys.argv) > 2  and ((sys.argv[1] == '-n' and len(sys.argv) > 2) or (sys.argv[2] == '-n' and len(sys.argv) > 3) \
	or (sys.argv[3] == '-n' and len(sys.argv) > 4)):
		norm = True
	else:
		sys.stderr.write('argument error')
		print Usage
		quit()

if '-l' in sys.argv:
	if len(sys.argv) > 2  and ((sys.argv[1] == '-l' and len(sys.argv) > 2) or (sys.argv[2] == '-l' and len(sys.argv) > 3) \
	or (sys.argv[3] == '-l' and len(sys.argv) > 4)):
		local = True
	else:
		sys.stderr.write('argument error')
		print Usage
		quit()			
		
if '-k' in sys.argv:
	if len(sys.argv) > 2  and ((sys.argv[1] == '-k' and len(sys.argv) > 2) or (sys.argv[2] == '-k' and len(sys.argv) > 3) \
	or (sys.argv[3] == '-k' and len(sys.argv) > 4)):
		k = True
	else:
		sys.stderr.write('argument error')
		print Usage
		quit()
		
if '-a' in sys.argv:
	if local:
		sys.stderr.write('argument error: -l and -a')
		print Usage
		quit()
	else:
		allFiles = True
if '-o' in sys.argv:
	assembleOnly = True
elif '-c' in sys.argv:
	currentFiles = True

if not allFiles and not currentFiles and norm and k and local:
	if not assembleOnly:
		fileList = sys.argv[4:]
	else: 
		fileList = sys.argv[5:]
elif not allFiles and not currentFiles and not norm and not k and not local:
	if not assembleOnly:
		fileList = sys.argv[1:]
	else: 
		fileList = sys.argv[2:]
elif not allFiles and not currentFiles and ((k and norm) or (k and local) or (norm and local)): 
	if not assembleOnly:
		fileList = sys.argv[3:]
	else: 
		fileList = sys.argv[4:]
elif not allFiles and not currentFiles and (k or norm or local): 
	if not assembleOnly:
		fileList = sys.argv[2:]
	else: 
		fileList = sys.argv[3:]
	
sample_dir = '/gpfs/pace1/project/bio-stewart-repository/SequencedDatasets/'
raw_dir = '/nv/hp10/nblackwood3/data/rawSeq/'

if not allFiles and not currentFiles:
	myfile = fileList[0]
	if not local and not os.path.isfile(os.path.join(sample_dir, myfile)):
		print myfile
		sys.stderr.write('file not found in repository')
		print Usage
		quit()
	elif local and not os.path.isfile(myfile):
		print myfile
		sys.stderr.write('file not found in current directory')
		print Usage
		quit()

###print 'all?: %s' % allFiles
###print 'current?: %s' % currentFiles
###print 'local file?: %s' % local 
###print 'normalize?: %s' % norm
###print 'change K?: %s' % k
###print 'file list: %s' % fileList
###print 'file: %s' % myfile

if not local:
	###print 'not local'
	zipped = False
	#check if file.fq.gz is already in /rawSeq
	if os.path.isfile(os.path.join(raw_dir, myfile)):
		print 'zipped file: %s in rawSeq already' % myfile
		zipped = True
	else:
		#check if unzipped file is already in rawSeq
		unzname, ext2 = os.path.splitext(myfile)
		if os.path.isfile(os.path.join(raw_dir, unzname)):
			unFile, ext3 = os.path.splitext(unzname)
			zipped= False
			print 'unzipped file: %s in rawSeq already' % unzname
		else: 
			#copy file.gz into raw Seq directory
			shutil.copy2(os.path.join(sample_dir, myfile), raw_dir)
			print 'file: %s copied' % myfile
			zipped = True
	#check for extension
	if zipped == True:
		outfilename, ext = os.path.splitext(myfile)
		unFile, ext = os.path.splitext(outfilename)
		print 'outfilename: %s' % outfilename
		print 'ext: %s' % ext
		if ext == '.gz':
			print 'file: %s is gzipped' % myfile
			#if zipped == True: # dont need
			subprocess.call(['gunzip', os.path.join(raw_dir, myfile)])
			print 'file: %s unzipped to: %s' % (myfile, outfilename)
		elif ext == '.tar':
			print 'file: %s is a tarball' % myfile
			#if zipped == True: # dont need
			#FIO
			print 'file: %s expanded to: %s' % (myfile, outfilename)
else:
	if os.path.isfile(os.path.join(raw_dir, myfile)):
		print 'file: %s in rawSeq already' % myfile
	else:
		#copy files into raw Seq directory
		path = '/nv/hp10/nblackwood3/data/scripts/%s' % (myfile)
		shutil.copy2(path, raw_dir)
		print 'file: %s copied' % myfile
	unFile, ext = os.path.splitext(myfile)
print 'without ext: %s' % unFile
print 'ready to assemble'

#clean with sickle 
sickle = '''#Script for Sickle
#PBS -N sickle
#PBS -l nodes=1:ppn=20
#PBS -l mem=20gb
#PBS -l walltime=90:00:00
#PBS -q microbio-1
#PBS -j oe
#PBS -o /nv/hp10/nblackwood3/data/logs/sickle/%s_sickle.log
#PBS -m abe
#PBS -M nigel.blackwood@biosci.gatech.edu

/nv/hp10/nblackwood3/data/sickle-master/sickle pe -c /nv/hp10/nblackwood3/data/rawSeq/%s.fq -t sanger -M /nv/hp10/nblackwood3/data/cleaned/%s_cleaned.fq\n''' % (unFile, unFile, unFile)

#make sickle script file
sicklefilename = "sickle_%s.pbs" % unFile
sile = open(sicklefilename, 'w')
sile.write(sickle)
sile.close()

#sile = open(sicklefilename, 'rU')
#for line in sile:
	#print lines
#sile.close()

argst = 'qsub %s' % sicklefilename
print argst
#args1 = shlex.split('qsub %s' % sicklefilename)
#print args1
subprocess.call(argst, shell=True)
print 'submitted Sickle script'
#print sickle

#open pipe to qsub
#p = subprocess.Popen('qsub', stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
#p.communicate(input=sickle)
#print 'submitted Sickle script'


#normalization
if norm:
	BBNorm = '''#BBNorm Script for Automation\n
#PBS -N megahit-1\n
#PBS -l nodes=1:ppn=20\n
#PBS -l mem=20gb\n
#PBS -l walltime=90:00:00\n
#PBS -q microbio-1\n
#PBS -j oe\n
#PBS -o /nv/hp10/nblackwood3/data/logs/BBNorm/%s_BBNorm.log\n
#PBS -m abe\n
#PBS -M nblackwood3@biosci.gatech.edu\n
/nv/hp10/nblackwood3/data/bbmap/bbnorm.sh in=/nv/hp10/nblackwood3/data/cleaned/%s_cleaned.fq out= /nv/hp10/nblackwood3/data/normed/%s_normed.fq interleaved=TRUE''' % (unFile, unFile, unFile)


	p = subprocess.Popen('qsub', stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True) % jobID
	p.communicate(BBnorm)
	p.stdin.close()
	print 'submitted BBNorm script'
	###print BBNorm
	
	if k:
		megaKN = '''#First Blast job of MILOCO samples\n
#PBS -N megahit-clean\n
#PBS -l nodes=1:ppn=20\n
#PBS -l mem=20gb\n
#PBS -l walltime=90:00:00\n
#PBS -q microbio-1\n
#PBS -j oe\n
#PBS -o /nv/hp10/nblackwood3/data/logs/MegaKN/%s_megaKN.log\n
#PBS -m abe\n
#PBS -M nblackwood3@biosci.gatech.edu\n
/nv/hp10/nblackwood3/data/megahit-master/megahit --12 /nv/hp10/nblackwood3/data/normed/%s_normed.fq --k-min 93 --k-max 133 --k-step 10 -o /nv/hp10/nblackwood3/data/finished/%s_assembledKN --min-contig-len 2000''' % (unFile, unFile, unFile)
		
		p = subprocess.Popen('qsub', stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
		p.communicate(megaKN)
		p.stdin.close()
		print 'submitted megaKN script'
		###print megaKN
	
	else:
		megaN = '''#Megahit Normed Script for Automization\n
#PBS -N megahit-MG300SV\n
#PBS -l nodes=1:ppn=20\n
#PBS -l mem=20gb\n
#PBS -l walltime=90:00:00\n
#PBS -q microbio-1\n
#PBS -j oe\n
#PBS -o /nv/hp10/nblackwood3/data/logs/MegaN/%s_megaN.log\n
#PBS -m abe\n
#PBS -M nblackwood3@biosci.gatech.edu\n
/nv/hp10/nblackwood3/data/megahit-master/megahit --12 /nv/hp10/nblackwood3/data/normed/%s_normed.fq -o /nv/hp10/nblackwood3/data/finished/%s_assembledN''' % (unFile, unFile, unFile)
	
		p = subprocess.Popen('qsub', stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
		p.communicate(megaN)
		p.stdin.close()
		print 'submitted megaN script'
		###print megaN
else:
	if k:
		megaK = '''#First Blast job of MILOCO samples\n
#PBS -N megahit-clean\n
#PBS -l nodes=1:ppn=20\n
#PBS -l mem=20gb\n
#PBS -l walltime=90:00:00\n
#PBS -q microbio-1\n
#PBS -j oe\n
#PBS -o /nv/hp10/nblackwood3/data/logs/MegaK/%s_megaK.log\n
#PBS -m abe\n
#PBS -M nblackwood3@biosci.gatech.edu\n
/nv/hp10/nblackwood3/data/megahit-master/megahit --12 /nv/hp10/nblackwood3/data/cleaned/%s_cleaned.fq --k-min 93 --k-max 133 --k-step 10 -o /nv/hp10/nblackwood3/data/finished/%s_assembledK --min-contig-len 2000''' % (unFile, unFile, unFile)
		
		p = subprocess.Popen('qsub', stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
		p.communicate(megaK)
		p.stdin.close()
		print 'submitted megaK script'
		###print megaK
	
	else:
		#write meta_script 
		mega = '''MegaHit Script for Automation
#PBS -N mega
#PBS -l nodes=1:ppn=20
#PBS -l mem=20gb
#PBS -l walltime=90:00:00
#PBS -q microbio-1
#PBS -j oe
#PBS -o /nv/hp10/nblackwood3/data/logs/Mega/%s_mega.log
#PBS -m abe
#PBS -M nblackwood3@biosci.gatech.edu

/nv/hp10/nblackwood3/data/megahit-master/megahit --12 /nv/hp10/nblackwood3/data/cleaned/%s_cleaned.fq -o /nv/hp10/nblackwood3/data/finished/%s_assembled\n''' % (unFile, unFile, unFile)
		
		#parse previous job ID from qstat
		print 'parsing jobID from Sickle Job'
		jobID = ''
		args = shlex.split('qstat -u nblackwood3')
		#print args
		p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
		out, err = p.communicate()
		#print 'output:'
		#print out
		rec = 0 
		for ch in out:
			if rec >= (len(out) - 114) and rec <= (len(out) - 108):
				jobID += ch
			#print ch, rec
			rec += 1
		#print 'length:'
		#print len(out)
		
		print 'jobID: %s' % jobID
		
		#make sickle script file
		megafilename = "mega_%s.pbs" % unFile
		mile = open(megafilename, 'w')
		mile.write(mega)
		mile.close()
		
		#added afterok to wait until cleaning done until submit new job
		#	or parsing Qstat resutls and waiting?
		args2 = "qsub -q iw-shared -W depend=afterok:%s %s" % (jobID, megafilename)
		print args2
		subprocess.call(args2, shell=True)
		print 'submitted mega script'

		'''
		args2 = shlex.split('qsub -q microbio-1 -W depend=afterok:%s' % jobID)
		#print args2
		p2 = subprocess.Popen(args2, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
		p2.communicate(input=mega)
		print 'submitted mega script'
		'''
		
		#mile = open(megafilename, 'rU')
		#for line in mile:
			#print lines
		#mile.close()

		#print mega
		
if assembleOnly == True:
	print "perform next steps"
else:
	print "assembly comleted"		
'''
#ROCker nar gene pooling
#CD local
if norm:
	if k:
		assembledFilePath = "/nv/hp10/nblackwood3/data/finished/%s_assembledKN" % unFile
	else:
		assembledFilePath = "/nv/hp10/nblackwood3/data/finished/%s_assembledN" % unFile
else:
	if k:
		assembledFilePath = "/nv/hp10/nblackwood3/data/finished/%s_assembledK" % unFile
	else:
		assembledFilePath = "/nv/hp10/nblackwood3/data/finished/%s_assembled" % unFile

ROCcmd= "ROCker search -q %s/final.contigs.fa -k NarG.250.rocker -o %s_ROCker.out" % (assembledFilePath, unFile)
convertCmd = "BlastToFasta.py %s_ROCker.out %s/final.contigs.fa %s_NarPool.fa" % (unFile, assembledFilePath, unFile)
os.system(ROCcmd)
os.system(convertCmd)

#nar Taxonomy
#CD Bacphile
#copy %s_NarPool.fa to Bacphile or save output to Blast in Bacphile correct directory
blastCmd = "/data/home/abertagnolli3/ncbi-blast-2.3.0+/bin/blastx -query %s_NarPool.fa -db ./nr -out ./%s_tax.txt -outfmt 6 -max_target_seqs 1 -num_threads 24" % (unFile, unFile)
idsCmd = "cut -f 2 %s_tax.txt > %s_ids.txt
namesCmd = "blastdbcmd -entry_batch ./%s_ids -db ./nr -outfmt %t -out ./%s_names" % (unFile, unFile)
countCmd = "uniq -c %s_names > %s_count" %  (unFile, unFile)
#^ maybe modify above output file destination
os.system(blastCmd)
os.system(idsCmd)
os.system(namesCmd)
os.system(countCmd)

#get contig length
inFileName = "%s_NarPool.fa" % unFile
outFileName = "%s_lengths.txt" % unFile
inFile = open(inFileName, 'rU')
outFile = open(outFileName, 'w')
grepString = ">(.*) flag=\d multi=[\d\.]* len=(\d*)"
for line in inFile:
	if line[0] == >:
		Result = re.search(grepString, line)
		outFile.write("%s\t%s\n") % (Result.group(1), Result.group(2))
inFile.close()
outFile.close()

#related genes
#add protCog to Bacphile
RblastCmd = "/data/home/abertagnolli3/ncbi-blast-2.3.0+/bin/blastx -query %s_ROCker.out -db ./db/blast/protCOG.fa -out ./%s_related.txt -outfmt 6 -num_threads 24" % (unFile, unFile)
RidsCmd = "cut -f 2 %s_related.txt > %s_Rids.txt
RnamesCmd = "blastdbcmd -entry_batch ./%s_Rids -db ./db/blast/protCOG.fa -outfmt %t -out ./%s_Rnames" % (unFile, unFile)
RsortCmd = 	"uniq -c Rnames_%s | sort -n -r > Rsorted_%s" % (unFile, unFile)
os.system(RblastCmd)
os.system(RidsCmd)
os.system(RnamesCmd)
os.system(RsortCmd)

#Tabulate
#lengths table: %s_lengths.txt
makeCmd = "tablesss.py %s_related.out Rnames_%s %s_Rtable.txt" % (unFile, unFile)
'''