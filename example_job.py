#!/usr/bin/python
#
# Created by Aaron Beckett January, 2016
#

import sys
import getopt
import time

cat = """
 _
( \\
 ) )
( (  .-""-.  A.-.A
 \ \/      \/ , , \\
  \   \    =;  t  /=
   \   |"".  ',--'
    / //  | ||
   /_,))  |_,))
"""

elephant = """
          __     __
         /  \~~~/  \\
   ,----(     ..    )
  /      \__     __/
 /|         (\  |(
^ \   /___\  /\ |
   |__|   |__|-"
"""

frog = """
  @..@
 (----)
( >__< )
^^ ~~ ^^
"""

canadian_flag = """
 _____________
|||    .    |||
|||  _/|\_  |||
|||  \\\\|//  |||
|||  '-|-'  |||
'''---------'''
"""

def main(argv):
    #
    # Get command line args with getopt
    #
    shortArgs = "t:f:"
    longArgs = ["wait-time=", "config-file="]
    opts,args = getopt.getopt(argv[1:], shortArgs, longArgs)

    waitTime = 0
    configFileName = ""
    for opt, arg in opts:
        if opt in ("-t", "--wait-time"):
            waitTime= arg
        elif opt in ("-f", "--config-file"):
            configFileName = arg

    #
    # Parse the config file to determine what to draw
    #
    draw_cat = False
    draw_elephant = False
    draw_frog = False
    draw_canadian_flag = False
    out_file = ""
    with open(configFileName) as cfg:
        for line in cfg:
            line = line.split()
            if line[0] == "cat" and line[1] == "on":
                draw_cat = True;
            elif line[0] == "elephant" and line[1] == "on":
                draw_elephant = True;
            elif line[0] == "frog" and line[1] == "on":
                draw_frog = True;
            elif line[0] == "canadian_pride" and line[1] == "on":
                draw_canadian_flag = True;
            elif line[0] == "out_file":
                out_file = line[1]

    #
    # Wait the given number of seconds
    #
    time.sleep(float(waitTime))

    #
    # Create the file with the drawings
    #
    with open(out_file, 'w') as out:
        if draw_cat:
            out.write(cat)
        if draw_elephant:
            out.write(elephant)
        if draw_frog:
            out.write(frog)
        if draw_canadian_flag:
            out.write(canadian_flag)

if __name__ == "__main__":
    main(sys.argv)

