#!/usr/local/bin/gnuplot --persist

FILENAME = ARG1
TITLE = ARG2
OUTPUT = ARG3

stats FILENAME nooutput
num_columns = STATS_columns

set terminal pdfcairo font "Times New Roman,24" size 8,6
set output OUTPUT
set title TITLE
plot_command = "plot FILENAME using 1:2 with linespoints title ARG4"

if (num_columns > 2) {
    do for [i=3:num_columns] {
        plot_command = plot_command . ", '' using 1:".i." with linespoints title ".sprintf('ARG%d',i+2)
    }
}
print plot_command

eval plot_command
