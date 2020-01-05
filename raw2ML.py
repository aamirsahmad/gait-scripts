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

    for sample in samples_list:
        # print('here is a gait sample after segmentation')
        # print(sample)
        dl_ready_sample = dt.sampling_and_interpolation(sample)
        # print('GAIT data after sampling and linear interpolation')
        # print(dl_ready_sample)
        dl_sample_list.append(dl_ready_sample)

    # print('total number of samples : ' + str(len(dl_sample_list)))

    return dl_sample_list


def parse_args(self):
    parser = ArgumentParser()
    parser.add_argument("filename")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    file = args.filename
    str_data = ""
    dl_sample_list = data_processing_driver(str_data)


if __name__ == "__main__":
    main()
