import sys
import argparse
from array import array

import os
from glob import glob
import shutil

def getFileList(path, pattern="*.slcio"):
    FileList = []

    #return nothing if path is a file
    if os.path.isfile(path):
       return []

    FileList = sorted(glob(os.path.join(path,pattern)))

    return FileList

if __name__== "__main__":

    parser = argparse.ArgumentParser(description='QUBO preselection Simplified LUXE',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--slcio',
                        action='store',
                        type=str,
                        help='PTARMIGAN file with .slcio ending')
    parser.add_argument('--xml',
                        help='detector description file .xml',
                        metavar='xml',
                        default="${luxegeo_DIR}/LUXETracker.xml")
    parser.add_argument('--nfiles', 
                        help='the number of file to be processed per batch_job',
                        type=int, default=1)
    parser.add_argument('--outdir', help='output directory', metavar='outdir')



    args = parser.parse_args()

    if (not args.slcio):
        print("No PTARMIGAN .slcio files provided! Exiting...")
        exit()

    list_of_h5_files = getFileList(args.slcio)
    outdir = os.path.abspath(args.outdir)
    logs = os.path.abspath(f"{outdir}/logs")

    if os.path.exists(f"{outdir}"):
        answer = input(f"{outdir} already exists, do you want to delete it? Please enter 'yes' or 'no':")
        if answer.lower().strip()[0] == "y":
            print(f"removing the old output dir {outdir}")
            shutil.rmtree(outdir)
        elif answer.lower().strip()[0] == "n":
            print(" You decided to keep the old out dir ")

    if not os.path.exists(f"{outdir}"):
        os.makedirs(f"{outdir}")

    if not os.path.exists(f"{logs}"):
        os.makedirs(f"{logs}")

    temp_list_of_files = []
    for i, file_ in enumerate(list_of_h5_files):
        temp_list_of_files.append(file_)
        if ((i !=0) and ((i+1) %( args.nfiles) == 0)) or (i == len(list_of_h5_files)-1):
            print(f"h5_to_slcio sumitting job for the following list of files:{temp_list_of_files}")

            confDir = os.path.join(outdir,"job_"+str(i+1))
            if not os.path.exists(confDir) : 
                os.makedirs(confDir)
                

            exec_ = open(confDir+"/exec.sh","w+")
            exec_.write("#"+"!"+"/bin/bash"+"\n")
            # exec_.write("eval "+'"'+"export PATH='"+path+":$PATH'"+'"'+"\n")
            # exec_.write("source "+anaconda+" hepML"+"\n")
            exec_.write("source /cvmfs/ilc.desy.de/key4hep/releases/2023-05-23/key4hep-stack/2023-05-24/x86_64-centos7-gcc12.3.0-opt/7emhu/setup.sh"+"\n")

            exec_.write("cd "+confDir+"\n")

            exec_.write("echo 'running job' >> "+confDir+"/processing"+"\n")

            exec_.write("source /nfs/dust/ilc/user/amohamed/luxegeo/install/bin/thisluxegeo.sh"+"\n")
            exec_.write("export luxegeo_DIR=/nfs/dust/ilc/user/amohamed/luxegeo/install/share/luxegeo/compact"+"\n")

            exec_.write("echo "+confDir+"\n")
            exec_.write("\n")
            for sfile in temp_list_of_files: 

                exec_.write(f"ddsim --compactFile {args.xml} \\"+"\n")
                exec_.write("--numberOfEvents -1 \\"+"\n")
                exec_.write(f"--inputFiles {os.path.abspath(sfile)} \\"+"\n")
                exec_.write(f"--outputFile {sfile.split('/')[-1].replace('.slcio','_edm4hep.root')}"+"\n")
                exec_.write("\n")

            # let the script deletes itself after finishing the job
            exec_.write(f"rm -rf {confDir}/processing"+"\n")
            exec_.write("echo 'done job' >> "+confDir+"/done"+"\n")              
            exec_.close()
            temp_list_of_files = []

        else: 
            continue

    subFilename = os.path.join(outdir,"submitAllJobs.conf")
    subFile = open(subFilename,"w+")
    subFile.write("executable = $(DIR)/exec.sh"+"\n")
    subFile.write("universe =  vanilla")
    subFile.write("\n")
    subFile.write("should_transfer_files = YES")
    subFile.write("\n")
    subFile.write("log = "+"{}/job_$(Cluster)_$(Process).log".format(os.path.abspath(logs)))
    subFile.write("\n")
    subFile.write("output = "+"{}/job_$(Cluster)_$(Process).out".format(os.path.abspath(logs)))
    subFile.write("\n")
    subFile.write("error = "+"{}/job_$(Cluster)_$(Process).err".format(os.path.abspath(logs)))
    subFile.write("\n")
    subFile.write("when_to_transfer_output   = ON_EXIT")
    subFile.write("\n")
    subFile.write('Requirements  = ( OpSysAndVer == "CentOS7")')
    subFile.write("\n")
    # subFile.write("+RequestRuntime = 6*60*60")
    subFile.write("\n")
    subFile.write("queue DIR matching dirs "+outdir+"/job_*/")
    subFile.close()
    submit_or_not = input(f"{i+1} jobs created, do you want to submit? Please enter 'yes' or 'no':")
    if submit_or_not.lower().strip()[0] == "y":
        os.system("condor_submit "+subFilename)
    elif submit_or_not.lower().strip()[0] == "n":
        print(f" You decided not to submit from the script, you may go to {outdir}; excute #condor_submit {subFilename}")

