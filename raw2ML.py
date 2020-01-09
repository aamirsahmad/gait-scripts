from argparse import ArgumentParser
import data_transformation as dt


def data_processing_driver(str_data):
    dl_sample_list = []
    peak_map = dt.find_all_peaks_in_partition(str_data)

    # print('peak_map length is : ' + str(len(peak_map)))

    # print('str data length is ' + str(len(str_data)))
    if (len(peak_map) < 5):
        return dl_sample_list
    # else:
    #     for (k,v) in peak_map.items():
    #         print('peak map items are:')
    #         print(k,v)

    samples_list = dt.gait_segmentation(str_data, peak_map)

    # print(len(samples_list))
    for sample in samples_list:
        # print('here is a gait sample after segmentation')
        # print(sample)
        dl_ready_sample = dt.sampling_and_interpolation(sample)
        # print('GAIT data after sampling and linear interpolation')
        # print(dl_ready_sample)
        dl_sample_list.append(dl_ready_sample)

    # print('total number of samples : ' + str(len(dl_sample_list)))

    return dl_sample_list


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("filename")
    args = parser.parse_args()
    return args

def generate_files(dl_sample_list):
    """
        res.append(userID)
        res.append(acc_x)
        res.append(acc_y)
        res.append(acc_z)

    """
    f1 = open('train_acc_x.txt', 'a+')
    f2 = open('train_acc_y.txt', 'a+')
    f3 = open('train_acc_z.txt', 'a+')
    f4 = open('train_id.txt', 'a+')

    f5 = open('train_gyr_x.txt', 'a+')
    f6 = open('train_gyr_y.txt', 'a+')
    f7 = open('train_gyr_z.txt', 'a+')
    
    print(len(dl_sample_list))
    for sample in dl_sample_list:
        # sys.exit()
        f4.write(str(sample[0]) + '\n')

        for d in sample[1]:
            f1.write(str(d) + ' ')
            f5.write('0' + ' ')
        f1.write('\n')
        f5.write('\n')


        for d in sample[2]:
            f2.write(str(d) + ' ')
            f6.write('0' + ' ')
        f2.write('\n')
        f6.write('\n')

        for d in sample[3]:
            f3.write(str(d) + ' ')
            f7.write('0' + ' ')
        f3.write('\n')
        f7.write('\n')

        # f1.write(row[1] + '\n')
        # f2.write(row[2] + '\n')
        # f3.write(row[3] + '\n')
        # f4.write(str(row[0]) + '\n')




    # # start the streaming computation
    # ssc.start()
    # # wait for the streaming to finish
    # ssc.awaitTermination()

    f1.close()
    f2.close()
    f3.close()
    f4.close()


def main():
    args = parse_args()
    file = args.filename
    str_data = ""
    with open(file, 'r') as f:
        for line in f:
            str_data += line

    dl_sample_list = data_processing_driver(str_data)
    generate_files(dl_sample_list)
    # print(dl_sample_list)


if __name__ == "__main__":
    main()
