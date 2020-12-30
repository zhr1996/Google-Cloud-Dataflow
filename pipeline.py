#!/usr/bin/env python3.7
###################################################################################################################################################
# Template written by David Cabinian and edited by Will Xia
# dhcabinian@gatech.edu
# wxia33@gatech.edu
# Written for python 3.7
# Run python template.py --help for information.
###################################################################################################################################################
# DO NOT MODIFY THESE IMPORTS / DO NOT ADD IMPORTS IN THIS NAMESPACE
# Importing a filesystem library such as ['sys', 'os', 'shutil'] will result in loss of all homework points.
###################################################################################################################################################
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
###################################################################################################################################################
import logging


def count(element):
    (ID, number) = element
    return (ID, sum(number))

# Format the counts into a PCollection of strings.


def format_result_int(purchase_count):
    (purchase, count) = purchase_count
    return '%s\t%d' % (purchase, count)


def format_result_float(purchase_count):
    (purchase, count) = purchase_count
    return '%s\t%f' % (purchase, count)


def process_game(element):
    (index, li) = element
    max_bought = -1
    result = []
    for tu in li:
        (game, count) = tu
        if game == index:
            continue
        if count == max_bought:
            result.append(game)
        elif count > max_bought:
            max_bought = count
            result = [game]

    if max_bought == -1:
        return (index + "\t" + "None" + "\t" + "0")
    else:
        result.sort(key=lambda x: int(x[2:]))
        output = index + "\t"
        for game in result:
            output += game + "\t"
        output += str(max_bought)
        return output


def convertPurchases(purchase):
    (purchaseID, games) = purchase
    games = list(games)
    result = []
    for game1 in games:
        for game2 in games:
            key = (game1, game2)
            value = 1
            result.append((key, value))
    return result


def run(args, pipeline_args):
    # INSERT YOUR CODE HERE
    import csv
    # pipeline_options = PipelineOptions(pipeline_args)
    # p = beam.Pipeline(options=pipeline_options)
    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        if (args.purchased_together):
            print(args.output)
            output = (
                p
                | 'Read from csv' >> beam.io.ReadFromText(args.input)
                | 'Use module to parse' >> beam.Map(lambda line: next(csv.reader([line], quotechar='"')))
                | 'Conver line into purchse tuple' >> beam.Map(lambda line: ((line[8] + line[11]), line[0]))
                | 'Group games in each purchase' >> beam.GroupByKey()
                | 'Convert purchase into game paris' >> beam.Map(convertPurchases)
                | 'Flatten the list' >> beam.FlatMap(lambda s: s)
                | 'Group by Game Paris' >> beam.CombinePerKey(sum)
                | 'Convert tuple to use first game as key' >> beam.Map(lambda element: (element[0][0], (element[0][1], element[1])))
                | 'Group by each gameID' >> beam.GroupByKey()
                | 'Convert to output' >> beam.Map(process_game)
                # | 'Print' >> beam.FlatMap(lambda line: print(line))
                | beam.io.WriteToText(args.output)
            )
        if (args.game_numbers and (not args.genre)):
            purchases = (p
                         #  | beam.Create([args.input])
                         #  | beam.FlatMap(read_csv)
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         # | 'Parse CSV' >> beam.Map(parse_csv)
                         | 'Cover to tuple' >> beam.Map(lambda purchase: (purchase[0], 1))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_int)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )
        if (args.game_revenue and (not args.genre)):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[0], float(purchase[10])))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_float)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )
        # output = purchases | 'format' >> beam.Map(format_result)
        if (args.developer_numbers and (not args.genre)):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[6], 1))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_int)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )

        if (args.developer_revenue and (not args.genre)):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[6], float(purchase[10])))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_float)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )

        # Calculate with filtering specific genre
        if (args.game_numbers and args.genre):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Filter genre' >> beam.Filter(lambda purchase: purchase[3] == args.genre)
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[0], 1))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_int)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )

        if (args.game_revenue and args.genre):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Filter genre' >> beam.Filter(lambda purchase: purchase[3] == args.genre)
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[0], float(purchase[10])))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_float)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )

        if (args.developer_numbers and args.genre):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Filter genre' >> beam.Filter(lambda purchase: purchase[3] == args.genre)
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[6], 1))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_int)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )

        if (args.developer_revenue and args.genre):
            purchases = (p
                         | 'Read CSV' >> beam.io.ReadFromText(args.input)
                         | 'Parse CSV' >> beam.Map(lambda line: next(csv.reader([line], skipinitialspace=True)))
                         | 'Filter genre' >> beam.Filter(lambda purchase: purchase[3] == args.genre)
                         | 'Convert to tuple' >> beam.Map(lambda purchase: (purchase[6], float(purchase[10])))
                         | 'Group' >> beam.GroupByKey()
                         | 'Count' >> beam.Map(count)
                         | 'format' >> beam.Map(format_result_float)
                         | beam.io.WriteToText(args.output)
                         # | 'Print' >> beam.FlatMap(lambda s: print(s))

                         )


###################################################################################################################################################
# DO NOT MODIFY BELOW THIS LINE
###################################################################################################################################################
if __name__ == '__main__':
    # This function will parse the required arguments for you.
    # Try template.py --help for more information
    # View https://docs.python.org/3/library/argparse.html for more information on how it works
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description="ECE 6102 Assignment 2",
                                     epilog="Example Usages:\npython test.py --input small_dataset.csv --output out.csv --runner Direct --game_numbers\npython test.py --input $BUCKET/input_files/small_dataset.csv --output $BUCKET/out.csv --runner DataflowRunner --project $PROJECT --temp_location $BUCKET/tmp/ --game_numbers")
    parser.add_argument(
        '--input', help="Input file to process.", required=True)
    parser.add_argument(
        '--output', help="Output file to write results to.", required=True)
    parser.add_argument('--project', help="Your Google Cloud Project ID.")
    parser.add_argument('--runner', help="The runner you would like to use for the map reduce.",
                        choices=['Direct', 'DataflowRunner'], required=True)
    parser.add_argument(
        '--temp_location', help="Location where temporary files should be stored.")
    parser.add_argument(
        '--num_workers', help="Set the number of workers for Google Cloud Dataflow to allocate (instead of autoallocation). Default value = 0 uses autoallocation.", default="0")
    pipelines = parser.add_mutually_exclusive_group(required=True)
    pipelines.add_argument('--game_numbers', help="Count and output the total number of purchased copies  for each game that appears in the data set. Each game has a unique ID, which can be matched to identify that two different transactions involve the same game.", action='store_true')
    pipelines.add_argument(
        '--game_revenue', help="Calculate and output the total revenue for each game. Again, use the game ID to match games to generate this data.", action='store_true')
    pipelines.add_argument('--developer_numbers', help="Count and output the total number of purchased copies for each game developer that appears in the data set.  Each developer has a unique ID, which can be matched to identify that two different transactions involve the same developer. ", action='store_true')
    pipelines.add_argument(
        '--developer_revenue', help="Calculate and output the total revenue for each developer. Again, use the developer ID to match developers to generate this data.", action='store_true')
    pipelines.add_argument('--purchased_together', help="For each game that was purchased at least once, find the other game that was most often purchased at the same time and count how many times the two games were part of the same transaction. Note that one transaction involving multiple games is entered as multiple lines in the data set. Games purchased in the same transaction have matching dates, times, and user IDs.", action='store_true')
    parser.add_argument('--genre', help="Use the genre according to the assignment description",
                        choices=["Action", "Adventure", "Puzzle", "Racing", "Role-playing", "Simulation", "Sports"])
    args = parser.parse_args()

    # Separating Pipeline options from IO options
    # HINT: pipeline args go nicely into: options=PipelineOptions(pipeline_args)
    if args.runner == "DataflowRunner":
        if None in [args.project, args.temp_location]:
            raise Exception("Missing some pipeline options.")
        pipeline_args = []
        pipeline_args.append("--runner")
        pipeline_args.append(args.runner)
        pipeline_args.append("--project")
        pipeline_args.append(args.project)
        pipeline_args.append("--temp_location")
        pipeline_args.append(args.temp_location)
        if args.num_workers != "0":
            # This disables the autoscaling if you have specified a number of workers
            pipeline_args.append("--num_workers")
            pipeline_args.append(args.num_workers)
            pipeline_args.append("--autoscaling_algorithm")
            pipeline_args.append("NONE")
    else:
        pipeline_args = []
    logging.getLogger().setLevel(logging.INFO)
    run(args, pipeline_args)
