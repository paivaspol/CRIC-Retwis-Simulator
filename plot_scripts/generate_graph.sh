#!/usr/bin/env bash

baseline_dir=$1
cric_dir=$2

if [[ "$#" -ne 2 ]];
then
  echo 'Usage: ./generate_graph.sh [baseline_dir] [cric_dir]'
  exit 1
fi

data_dirs=( ${baseline_dir} ${cric_dir} )

for data_dir in ${data_dirs[@]};
  do
  prefix='cric'
  if [ ${data_dir} = ${baseline_dir} ]
  then
    prefix='baseline'
  fi

  ops=( read write )
  for op in ${ops[@]};
  do
    cp ${data_dir}/${op}_latencies_1_3 ${op}_latencies_1_3
    sort -k1 -g ${op}_latencies_1_3 > sorted_${op}_latencies
    # cp ${data_dir}/${op}_latencies_2_5 ${op}_latencies_2_5
    # sort -k1 -g ${op}_latencies_2_5 > sorted_${op}_latencies
    # join the sorted_op_latencies on the client_ids
    start=`cat start_at_least_100_followers`
    awk -v start="${start}" '($1 >= start) { print $0 }' sorted_${op}_latencies > ${prefix}_${op}_latencies_at_least_100_followers
    start=`cat start_at_least_250_followers`
    awk -v start="${start}" '($1 >= start) { print $0 }' ${prefix}_${op}_latencies_at_least_100_followers > ${prefix}_${op}_latencies_at_least_250_followers
    # rm ${op}_latencies_1_3
    # rm sorted_${op}_latencies
  done
done

# Generate the difference and and CDF.
ops=( read write )
nums=( 100 250 )
for op in ${ops[@]};
do
  for num in ${nums[@]};
  do
  # join baseline_${op}_latencies_at_least_${num}_followers cric_${op}_latencies_at_least_${num}_followers \
  #   | awk '{ print $1 " " $3 " " $7 " " ($3 - $7) }' \
  #   | sort -k4 -g > latency_diff_${op}_at_least_${num}.txt
  join baseline_${op}_latencies_at_least_${num}_followers cric_${op}_latencies_at_least_${num}_followers \
    | awk '{ print $1 " " $4 " " $8 " " ($4 - $8) }' \
    | sort -k4 -g > latency_diff_${op}_at_least_${num}.txt

  # join baseline_${op}_latencies_at_least_${num}_followers cric_${op}_latencies_at_least_${num}_followers \
  #   | awk '{ print $1 " " $3 " " $7 " " (1.0 * ($3 - $7) / $3) }' \
  #   | sort -k4 -g > latency_rel_diff_${op}_at_least_${num}.txt
  join baseline_${op}_latencies_at_least_${num}_followers cric_${op}_latencies_at_least_${num}_followers \
    | awk '{ print $1 " " $4 " " $8 " " (1.0 * ($4 - $8) / $4) }' \
    | sort -k4 -g > latency_rel_diff_${op}_at_least_${num}.txt
  ~/useful_scripts/generate_cdf.sh latency_diff_${op}_at_least_${num}.txt
  ~/useful_scripts/generate_cdf.sh latency_rel_diff_${op}_at_least_${num}.txt
  done
done
gnuplot plot_cdf_difference
~/useful_scripts/convert_to_pdf.sh cdf_latency_difference.ps
gnuplot plot_cdf_relative_difference
~/useful_scripts/convert_to_pdf.sh cdf_rel_latency_difference.ps
open *.pdf
