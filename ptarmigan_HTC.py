# import htcondor  
import os  
import shutil
import argparse

# schedd = htcondor.Schedd()  
# sub = htcondor.Submit("")

wdir = "/nfs/dust/ilc/user/amohamed/ptarmigan"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Runs a NAF batch system for LUXESIM', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--outdir', help='output directory', metavar='outdir')
    parser.add_argument('--nevents', help='the number of events to be generated', type=int, default=1)
    parser.add_argument('--cfg', help="config_file for expriment in the form of yml file", default='/nfs/dust/ilc/user/amohamed/HTC/e0gpc_7.0_0000.yml', metavar='cfg')

    args = parser.parse_args()
    
    outdir = os.path.abspath(args.outdir)
    logs = os.path.abspath(f"{outdir}/logs")
    cfg = os.path.abspath(args.cfg)
    
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

    events_list = ["%04d" % x for x in range(args.nevents)]

    for i, event in enumerate(events_list):
        cfg_ = cfg
        cfg_ = cfg_.split("/")[-1].replace("0000",event)
        if i % 500 == 0:
            print(f"{i} jobs created")
        confDir = os.path.join(outdir,"job_"+str(event))
        if not os.path.exists(confDir) : 
            os.makedirs(confDir)
        
        shutil.copyfile(args.cfg, f"{confDir}/{cfg_}")

        exec_ = open(confDir+"/exec.sh","w+")
        exec_.write("#"+"!"+"/bin/bash"+"\n")
        # exec_.write("eval "+'"'+"export PATH='"+path+":$PATH'"+'"'+"\n")
        # exec_.write("source "+anaconda+" hepML"+"\n")
        exec_.write("source /cvmfs/ilc.desy.de/key4hep/releases/2023-05-23/key4hep-stack/2023-05-24/x86_64-centos7-gcc12.3.0-opt/7emhu/setup.sh"+"\n")

        exec_.write("cd "+wdir+"\n")

        exec_.write("echo 'running job' >> "+confDir+"/processing"+"\n")

        exec_.write("echo "+wdir+"\n")

        exec_.write(f"./target/release/ptarmigan {confDir}/{cfg_}")
        exec_.write("\n")
        # let the script deletes itself after finishing the job
        exec_.write(f"rm -rf {confDir}/processing"+"\n")
        exec_.write("echo 'done job' >> "+confDir+"/done"+"\n")
        exec_.close()
    
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
    subFile.write("+RequestRuntime = 6*60*60")
    subFile.write("\n")
    subFile.write("queue DIR matching dirs "+outdir+"/job_*/")
    subFile.close()
    submit_or_not = input(f"{i+1} jobs created, do you want to submit? Please enter 'yes' or 'no':")
    if submit_or_not.lower().strip()[0] == "y":
        os.system("condor_submit "+subFilename)
    elif submit_or_not.lower().strip()[0] == "n":
        print(f" You decided not to submit from the script, you may go to {outdir}; excute #condor_submit {subFilename}")
    # 


