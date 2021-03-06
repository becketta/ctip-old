#
# Created by Aaron Beckett January, 2016
#
import os, sys
from subprocess import Popen, PIPE
from multiprocessing import Queue

from ctip_utils import QsubBuilder
from ctip_constants import CTIP_ROOT

################################################################
# Set this variable to the name of your templated qsub file
################################################################
templated_qsub_file = CTIP_ROOT + "example_qsub.qsub"


def runConfig(config, queue, outdir="", qsub=templated_qsub_file):
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

    # Generate the config file name for this run
    cfg_file = os.path.join(runDir, runName + ".cfg")


    ################################################################
    # EDIT CREATION OF CONFIG FILE:
    ################################################################

    # Fill lines list with lines to add to EVERY config file
    out_file_name = os.path.join(runDir, "ascii_images")
    lines = [ "out_file {0}\n".format(out_file_name) ]

    # Loop over all columns in the config
    for key in config.keys():
        # Skip the special cols of 'id' and 'tag' that are used by ctip
        if key not in ('id', 'tag', 'wait_time'):
            # Create a formated line to add to the config file for
            # this key, value pair
            lines.append("{0} {1}\n".format(key, config[key]))

    ################################################################



    # Write the lines list to the config file
    with open(cfg_file, 'w') as cfg:
        cfg.writelines(lines)



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
            'wait_time': config['wait_time']
        }

    ################################################################



    # Create custom string Template from the qsub template
    with open(qsub, 'r') as qsub_template:
        template = QsubBuilder(qsub_template.read())

    # Create the temporary qsub file from the template using subst_dict
    run_qsub = os.path.join(runDir, runName + ".qsub")
    with open(run_qsub, 'w') as temp_qsub:
        qsub_text = template.substitute(subst_dict)
        temp_qsub.write(qsub_text)

    #
    # Submit the job to the scheduler using the temporary qsub file
    #
    proc = Popen(['qsub', run_qsub], stdout=PIPE)
    job_id = proc.stdout.read()
    job_id = job_id.strip().split('.')
    queue.put( (job_id[0], runName) )

