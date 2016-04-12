#
# Created by Aaron Beckett January, 2016
#
import os, sys
from subprocess import Popen, PIPE
from multiprocessing import Queue

from ctip_utils import QsubBuilder
from ctip_constants import QSUB_TEMPLATE, CFG_TEMPLATE

################################################################
# Set this variable to the name of your templated qsub file
################################################################

def runConfig(config, queue, outdir="", qsub_file=QSUB_TEMPLATE):
    """Run a program using given configuration"""

    # Build the directory for this run
    try:
        runName = str(config['id'])
    except IndexError:
        print("***ERROR***")
        print("Unable to start job, no id specified in config:")
        sys.exit(1)
    # If there is a tag column in the config, add it to the run name
    try:
        if config['tag']:
            runName += "_" + config['tag']
    except IndexError:
        pass
    runDir = os.path.join(outdir, runName)
    os.makedirs(runDir)



    cfg_file = os.path.join(runDir, runName + ".cfg")

    ################################################################
    # EDIT CREATION OF CONFIG FILE:
    ################################################################

    subst_dict = {
        'job_dir': runDir,
        'world': config['world'],
        'decoder': config['decoder'],
        'gates': config['gates'],
        'gate_complexity': config['complexity'],
        'trials': config['trials'],
        'length': config['length'],
        'optimizer': config['optimizer'],
        'eval_limit': config['eval_limit'],
        'fitness_limit': config['fitness_limit']
    }

    ################################################################



    # Create custom string Template from the cfg template
    with open(CFG_TEMPLATE, 'r') as cfg_template:
        template = QsubBuilder(cfg_template.read())

    # Create the config file from the template using subst_dict
    with open(cfg_file, 'w') as cfg:
        cfg_text = template.substitute(subst_dict)
        cfg.write(cfg_text)



    ################################################################
    # MAY WANT TO EDIT:
    ################################################################

    # Match values to all keys that are in the qsub template
    subst_dict = {
        # Should probably leave these mappings as is
        'job_name': runName,
        'shell_out_file': os.path.join(runDir, runName + ".o"),
        'job_dir': runDir,

        # May want to edit these mappings and add your own, just
        # make sure there's a corresponding %=<key_name> in the
        # qsub template. For example, the below mapping replaces
        # all '%=config_file' strings in the qsub template with
        # the string stored in cfg_file
        'config_file': cfg_file,
    }

    ################################################################



    # Create custom string Template from the qsub template
    with open(qsub_file, 'r') as qsub_template:
        template = QsubBuilder(qsub_template.read())

    # Create the qsub file from the template using subst_dict
    run_qsub = os.path.join(runDir, runName + ".qsub")
    with open(run_qsub, 'w') as qsub:
        qsub_text = template.substitute(subst_dict)
        qsub.write(qsub_text)

    #
    # Submit the job to the scheduler using the temporary qsub file
    #
    proc = Popen(['qsub', run_qsub], stdout=PIPE)
    job_id = proc.stdout.read()
    job_id = job_id.strip().split('.')
    queue.put( (job_id[0], runName) )

