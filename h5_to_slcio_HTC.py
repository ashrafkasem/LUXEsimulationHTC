import sys
import argparse
from array import array

import os
from glob import glob
import shutil

def write_to_lcio(ptarmigan_file, outfile):

    def convert_to_MCParticle(particles):

        for j in range(len(ptarmigan_file[f'final-state/{particles}/momentum'])):
    
            px = ptarmigan_file[f'final-state/{particles}/momentum'][j][1]
            py = ptarmigan_file[f'final-state/{particles}/momentum'][j][2]
            pz = ptarmigan_file[f'final-state/{particles}/momentum'][j][3]
 
            momentum = array('f', [px, py, pz])

            vx = ptarmigan_file[f'final-state/{particles}/position'][j][1]
            vy = ptarmigan_file[f'final-state/{particles}/position'][j][2]
            vz = ptarmigan_file[f'final-state/{particles}/position'][j][3]

            vertex = array('d', [vx, vy, vz])

            endpoint = array('d', [px, py, pz])

            # --------------- create MCParticle -------------------

            mcp = IMPL.MCParticleImpl()

            mcp.setGeneratorStatus(genstat)
            mcp.setMomentum(momentum)
            mcp.setCharge(charge[particles])
            mcp.setPDG(PDG[particles])
            mcp.setVertex(vertex)
            mcp.setEndpoint(endpoint)

            col.addElement(mcp)


    wrt = IOIMPL.LCFactory.getInstance().createLCWriter()

    wrt.open(outfile, EVENT.LCIO.WRITE_NEW)

    print(" opened outfile: ", outfile)

    # ========== particle properties ===================

    genstat = 1

    # =================================================

    # write a RunHeader
    run = IMPL.LCRunHeaderImpl()
    run.parameters().setValue("Generator", "ptarmigan")
    wrt.writeRunHeader(run)
    # ================================================
    
    # adjust the event number for future ptarmig version different from 0.8.1
    event_number = int(outfile.split("_")[-2])

    col = IMPL.LCCollectionVec(EVENT.LCIO.MCPARTICLE)
    
    if args.positrons or args.all:
        print('Converting positrons...')
        convert_to_MCParticle('positron')    
    if args.electrons or args.all:
        print('Converting electrons...')
        convert_to_MCParticle('electron')
    if args.photons or args.all:
        print('Converting photons...')
        convert_to_MCParticle('photon')    

    evt = IMPL.LCEventImpl()
    evt.setEventNumber(event_number)
    evt.addCollection(col, "MCParticle")
    wrt.writeEvent(evt)

    wrt.close()

def getDirectoryList(path, pattern="*.h5"):
    directoryList = []

    #return nothing if path is a file
    if os.path.isfile(path):
        return []

    directoryList = sorted(glob(os.path.join(path,pattern)))

    return directoryList


parser = argparse.ArgumentParser(description='QUBO preselection Simplified LUXE',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('--ptarmigan',
                    action='store',
                    type=str,
                    help='PTARMIGAN file with .h5 ending')
parser.add_argument('--positrons', 
                    action='store_true',
                    help='Specify this if positrons should be converted')
parser.add_argument('--electrons', 
                    action='store_true',
                    help='Specify this if electrons should be converted')
parser.add_argument('--photons', 
                    action='store_true',
                    help='Specify this if photons should be converted')
parser.add_argument('--all', 
                    action='store_true',
                    help='Specify this if positrons, electrons and photons should be converted')
parser.add_argument('--batch', 
                    action='store_true',
                    help='run on DESY HTC batch system')
parser.add_argument('--nfiles', 
                    help='the number of file to be processed per batch_job',
                    type=int, default=1)
parser.add_argument('--outdir', help='output directory', metavar='outdir')



args = parser.parse_args()

if (not args.ptarmigan) and (not args.batch):
    print("No PTARMIGAN .h5 file provided! Exiting...")
    exit()

if (not args.ptarmigan) and (args.batch):
    print("No PTARMIGAN ditrctory provided! Exiting...")
    exit()

PDG = {'photon': 22,
       'electron': 11,
       'positron': -11}
charge = {'photon': 0,
          'electron': -1,
          'positron': 1}

if not args.batch: 
    
    from pyLCIO import EVENT, UTIL, IOIMPL, IMPL
    import h5py

    ptarmigan_file = h5py.File(args.ptarmigan, 'r')
    
    if args.positrons and not args.photons and not args.electrons:
        outfile = f"{'_'.join('.'.join(args.ptarmigan.split('/')[-1].split('.')[0: -1]).split('_')[0:-1])}_positrons"
    elif args.electrons and not args.photons and not args.positrons:
        outfile = f"{'_'.join('.'.join(args.ptarmigan.split('/')[-1].split('.')[0: -1]).split('_')[0:-1])}_electrons"
    elif args.photons and not args.positrons and not args.electrons:
        outfile = f"{'_'.join('.'.join(args.ptarmigan.split('/')[-1].split('.')[0: -1]).split('_')[0:-1])}_photons"  
    else:
        outfile = f"{'.'.join(args.ptarmigan.split('/')[-1].split('.')[0: -1])}"
    
    write_to_lcio(ptarmigan_file, outfile)

else: 
    list_of_h5_files = getDirectoryList(args.ptarmigan)
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
        if ((i !=0) and (i % args.nfiles == 0)) or (i == len(list_of_h5_files)-1):
            print(f"h5_to_slcio sumitting job for the following list of files:{temp_list_of_files}")

            confDir = os.path.join(outdir,"job_"+str(i))
            if not os.path.exists(confDir) : 
                os.makedirs(confDir)
            

            exec_ = open(confDir+"/exec.sh","w+")
            exec_.write("#"+"!"+"/bin/bash"+"\n")
            # exec_.write("eval "+'"'+"export PATH='"+path+":$PATH'"+'"'+"\n")
            # exec_.write("source "+anaconda+" hepML"+"\n")
            exec_.write("source /cvmfs/ilc.desy.de/key4hep/releases/2023-05-23/key4hep-stack/2023-05-24/x86_64-centos7-gcc12.3.0-opt/7emhu/setup.sh"+"\n")

            exec_.write("cd "+confDir+"\n")

            exec_.write("echo 'running job' >> "+confDir+"/processing"+"\n")

            exec_.write("echo "+confDir+"\n")
            for sfile in temp_list_of_files: 
                exec_.write(f"python {os.getcwd()}/h5_to_slcio_HTC.py --ptarmigan {os.path.abspath(sfile)} --positrons")
                exec_.write("\n")
                exec_.write("echo 'step II done job' >> "+os.path.abspath(os.path.dirname(sfile))+"/done_step_II"+"\n")
                
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

