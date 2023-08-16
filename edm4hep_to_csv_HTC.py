import sys

import argparse

from glob import glob
import shutil
import os

def get_edm4hep_path():
    """
    Get the EDM4hep path from LD_LIBRARY_PATH
    NOTE: not the most elegant, nor the most robust solution, but should in general 
    work with Key4hep environments
    """
    edm4hep_lib_path = [
        p for p in os.environ["LD_LIBRARY_PATH"].split(":") if "/edm4hep/" in p
    ][0]
    edm4hep_path = "/".join(edm4hep_lib_path.split("/")[:-1])
    return edm4hep_path


def getFileList(path, pattern="*.root"):
    FileList = []

    #return nothing if path is a file
    if os.path.isfile(path):
       return []

    FileList = sorted(glob(os.path.join(path,pattern)))

    return FileList


def doEvtLoop(inputfiles,maxEvt=-1):
    reader = root_io.Reader(inputfiles)
    event = reader.get("events")

    columns=["event","hit_id","particle_id","h_CellID","h_EDep","h_Time","h_PathLength",
            "h_Quality","h_isOverlay","h_x","h_y","h_z","h_px","h_py","h_pz","isProducedBySecondary",
            "p_PDG","p_Energy","p_Charge","p_Time","p_Mass","p_vx","p_vy","p_vz",
            "p_end_vx","p_end_vy","p_end_vz","p_px","p_py","p_pz","p_end_px",
            "p_end_py","p_end_pz","p_spin_x","p_spin_y","p_spin_z","p_isOverlay","p_isStopped",
            "p_isCreatedInSimulation","p_isBackscatter","p_vertexIsNotEndpointOfParent",
            "p_isDecayedInTracker","p_isDecayedInCalorimeter","p_hasLeftDetector"
            ]


    for i,e in enumerate(event): 
        hists = []           
        if maxEvt != -1 and i >= maxEvt:
            break

        EventHeader = e.get("EventHeader")[0]
        eventNumber = EventHeader.getEventNumber()

        SiHits = e.get("SiHits")
        print(f" number of Hits in the event number {i} is: {SiHits.size()}")
        
        truth_df = pd.DataFrame(columns=columns, index=range(SiHits.size()))
        print(f"dataframe created of shape {truth_df.shape}")
      
        for Hi, SiH in enumerate(SiHits):
            if Hi % 5000 == 0:
                print(f"processing the hit number {Hi}")
            if SiH.getMCParticle().getPDG() != -11 : continue 
            truth_df.iloc[Hi, : ] = [eventNumber, 
                                     SiH.getObjectID().index,
                                     SiH.getMCParticle().getObjectID().index,
                                     SiH.getCellID(),
                                     SiH.getEDep(),
                                     SiH.getTime(),
                                     SiH.getPathLength(),
                                     SiH.getQuality(),
                                     SiH.isOverlay(),
                                     SiH.getPosition()[0],
                                     SiH.getPosition()[1],
                                     SiH.getPosition()[2],
                                     SiH.getMomentum()[0],
                                     SiH.getMomentum()[1],
                                     SiH.getMomentum()[2],
                                     SiH.isProducedBySecondary(),
                                     SiH.getMCParticle().getPDG(),
                                     SiH.getMCParticle().getEnergy(),
                                     SiH.getMCParticle().getCharge(),
                                     SiH.getMCParticle().getTime(),
                                     SiH.getMCParticle().getMass(),
                                     SiH.getMCParticle().getVertex()[0],
                                     SiH.getMCParticle().getVertex()[1],
                                     SiH.getMCParticle().getVertex()[2],
                                     SiH.getMCParticle().getEndpoint()[0],
                                     SiH.getMCParticle().getEndpoint()[1],
                                     SiH.getMCParticle().getEndpoint()[2],
                                     SiH.getMCParticle().getMomentum()[0],
                                     SiH.getMCParticle().getMomentum()[1],
                                     SiH.getMCParticle().getMomentum()[2],
                                     SiH.getMCParticle().getMomentumAtEndpoint()[0],
                                     SiH.getMCParticle().getMomentumAtEndpoint()[1],
                                     SiH.getMCParticle().getMomentumAtEndpoint()[2],
                                     SiH.getMCParticle().getSpin()[0],
                                     SiH.getMCParticle().getSpin()[1],
                                     SiH.getMCParticle().getSpin()[2],
                                     SiH.getMCParticle().isOverlay(),
                                     SiH.getMCParticle().isStopped(),
                                     SiH.getMCParticle().isCreatedInSimulation(),
                                     SiH.getMCParticle().isBackscatter(),
                                     SiH.getMCParticle().vertexIsNotEndpointOfParent(),
                                     SiH.getMCParticle().isDecayedInTracker(),
                                     SiH.getMCParticle().isDecayedInCalorimeter(),
                                     SiH.getMCParticle().hasLeftDetector(),
                                     ]

    return truth_df


def main(outdir, inputfile):
    
    df = doEvtLoop(inputfile)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    df.to_pickle(f"{outdir}/{inputfile.split('/')[-1].replace('.root', '.tar.gz')}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Runs a NAF batch system for LUXESIM', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--rootfiles', help='input directory or inputfile if you are not using batch system', metavar='rootfiles')
    parser.add_argument('--nfiles', help='the number of events to be generated', type=int, default=1)
    parser.add_argument('--outdir', help="output directory",  metavar='outdir')
    parser.add_argument('--batch', help="activate batch system submission",  action='store_true')

    args = parser.parse_args()


    if (not args.rootfiles):
        print("No rootfiles are provided! Exiting...")
        exit()

    if not args.batch: 

        sys.argv.append( '-b-' )
        import ROOT
        ROOT.gROOT.SetBatch(True)
        sys.argv.remove( '-b-' )

        ROOT.gStyle.SetOptStat(0)
        # ROOT.gStyle.SetOptTitle(0)

        from ROOT import edm4hep
        import numpy as np
        
        from podio import root_io
        import pandas as pd

        ROOT.gInterpreter.LoadFile(get_edm4hep_path()+"/include/edm4hep/utils/kinematics.h")
        USE_ENERGY=edm4hep.utils.detail.UseEnergyTag()

        if not os.path.isfile(args.rootfiles): 
            print("you choosed to run in the interactive mode, you should provide a single root tile not a dir! Exiting...")
            exit()

        main(outdir = args.outdir , inputfile=args.rootfiles)

    else: 
        list_of_h5_files = getFileList(args.rootfiles, "*.root")
        outdir = os.path.abspath(args.outdir)
        logs = os.path.abspath(f"{outdir}/logs")

        if os.path.exists(f"{outdir}"):
            answer = input(f"{outdir} already exists, do you want to delete it? Please enter 'yes' or 'no':")
            if answer.lower().strip()[0] == "y":
                print(f"removing the old output dir {outdir}")
                shutil.rmtree(outdir)
            elif answer.lower().strip()[0] == "n":
                print("You decided to keep the old out dir ")

        if not os.path.exists(f"{outdir}"):
            os.makedirs(f"{outdir}")

        if not os.path.exists(f"{logs}"):
            os.makedirs(f"{logs}")
        temp_list_of_files = []
        for i, file_ in enumerate(list_of_h5_files):
            temp_list_of_files.append(file_)
            if ((i !=0) and ((i+1) % args.nfiles == 0)) or (i == len(list_of_h5_files)-1):
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

                exec_.write("echo "+confDir+"\n")
                for sfile in temp_list_of_files: 
                    exec_.write(f"python {os.getcwd()}/edm4hep_to_csv_HTC.py --rootfiles {os.path.abspath(sfile)} --outdir {confDir}")
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


