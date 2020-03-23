import math
from collections import OrderedDict

from io import StringIO
import pandas as pd

# NOTE: The print statements in this module are crucial for debugging (DO NOT remove)
# Consider replacing them with logger INFO/WARN

Y_AXIS_THRESHOLD = 28.00  #
SKIP_FRAMES = 35  # a good heuristics value for 50Hz can go up to 45


def find_all_peaks_in_partition(str_data):
    """
        data format:
        2
        195, 2, 1574839747019, -1.087339, 6.710845, 7.041358, 9.787669
        ...
        userID
        index,userID,timeMs,accX,accY,accZ,vSum

        segmented gait data format:
        (78, 5, 1574587264032, -1.403481, 4.89542, 8.473579, 9.886174);
        (index,userID,timeMs,accX,accY,accZ,vSum)
    """
    max_vsum = 0
    max_line_list = ''
    max_timestamp = 0
    last_timestamp = 0
    max_e2 = ''

    index = 1  # skip first

    peak_map = OrderedDict()
    try:
        str_data = iter(str_data.splitlines())

        next(str_data)
        e1, e2, e3 = next(str_data), next(str_data), next(str_data)
        is_peak = False

        for e3 in str_data:
            is_peak = False
            v1 = float((e1.split(',')[6]).strip())
            v2 = float((e2.split(',')[6]).strip())
            v3 = float((e3.split(',')[6]).strip())

            i2 = int((e2.split(',')[0]).strip())  # index of e2
            if (v1 < v2 and v3 < v2):
                # it's some peak
                peak_index = int((e2.split(',')[0]).strip())
                line = e2.replace("\n", "").split(",")
                index = line[0].strip()
                userId = line[1].strip()
                timestamp = line[2].strip()
                accelerometer_x = line[3].strip()
                accelerometer_y = line[4].strip()
                accelerometer_z = line[5].strip()
                vector_sum = line[6].strip()
                line_list = [index, str(userId), timestamp, accelerometer_x, accelerometer_y, accelerometer_z,
                                vector_sum]
                if(int(last_timestamp) == 0):
                    last_timestamp = timestamp
                if(float(vector_sum) > float(max_vsum)):
                    max_vsum = vector_sum
                    max_line_list = line_list
                    max_e2 = e2

                if ((int(timestamp) - int(last_timestamp)) > 1000):
                    max_vsum = 0
                    last_timestamp = max_timestamp
                    peak_map[i2] = True
                    # peaks.append(max_line_list)
                    # peakFile.write(max_e2)

            # move pointers
            e1 = e2
            e2 = e3
    except StopIteration:
        pass

    return peak_map

    #     for e3 in str_data:
    #         is_peak = False
    #         v1 = float((e1.split(',')[6]).strip())
    #         v2 = float((e2.split(',')[6]).strip())
    #         v3 = float((e3.split(',')[6]).strip())

    #         i2 = int((e2.split(',')[0]).strip())  # index of e2
    #         # print(v3)
    #         if (v1 < v2 and v3 < v2):
    #             # it's some peak
    #             if (v2 > Y_AXIS_THRESHOLD):
    #                 # it's a Real peak (since it's 'first' peak in the window)
    #                 # move the sliding window
    #                 peak_map[i2] = True
    #                 is_peak = True
    #                 # slide the window
    #                 for i in range(SKIP_FRAMES - 1):
    #                     next(str_data)
    #                 # adjust pointers
    #                 e1 = next(str_data)
    #                 e2 = next(str_data)

    #         # move pointers
    #         if (is_peak == False):
    #             e1 = e2
    #             e2 = e3
    # except StopIteration:
    #     pass
    # # print(peak_map)
    # return peak_map


def gait_segmentation(str_data, peak_map):
    """
        two samples strategy
        5 peaks to discover
    """
    samples_list = []  # result
    # each sample will be a csv like table string ready for pandas
    peaks_to_discover = len(peak_map)
    # index,userID,timeMs,accX,accY,accZ,vSum

    # state 0 : write data until peak, no new line, go to state 1
    # state 1 : write data until peak, add new line, write same data again go to state 1
    peak_map_len = len(peak_map)

    if (peak_map_len < 5):
        return samples_list

    peak_map_iter = iter(peak_map)

    first_peak_index = next(peak_map_iter)

    second_peak_index = next(peak_map_iter)

    str_data = iter(str_data.splitlines())
    next(str_data) # skip headings

    try:
        # discard data until first peak
        while True:
            data = next(str_data)
            index = int((data.split(',')[0]).strip())  # index of e2
            if index == first_peak_index:
                peaks_to_discover -= 1
                break

        # append data until second peak : EDGE CASE non overlapping
        res = 'index,userID,timeMs,accX,accY,accZ,vSum\n'
        while (True):
            data = next(str_data)
            index = int((data.split(',')[0]).strip())  # index of e2
            res += data + '\n'
            if (index == second_peak_index):
                peaks_to_discover -= 1
                break

        # append all data twice for each subsequent TWO peak
        prefix = ''
        next_peak_index = next(peak_map_iter)
        while (True):
            data = next(str_data)
            index = int((data.split(',')[0]).strip())  # index of e2
            res += data + '\n'
            prefix += data + '\n'
            if (index == next_peak_index):
                samples_list.append(res)
                res = 'index,userID,timeMs,accX,accY,accZ,vSum\n' + prefix
                prefix = ''
                next_peak_index = next(peak_map_iter)
                peaks_to_discover -= 1
            if (peaks_to_discover == 0):
                break
    except StopIteration:
        pass
    # print(samples_list)
    return samples_list


def sampling_and_interpolation(sample):
    """
        for each item in samples_list call this method
    """
    DATA_POINTS = 128  # per row for dataset#1
    DECIMAL_PLACES = 8  # per row for dataset#1
    res = []

    data_io_str = StringIO(sample)

    df = pd.read_csv(data_io_str, sep=",", squeeze=True)

    # print('pandas dataframe input for sampling_and_interpolation: ')
    # print(df)
    userID = df['userID'].iloc[0]

    start_time = df['timeMs'].iloc[0]
    end_time = df['timeMs'].iloc[-1]

    # print(start_time)
    # print(end_time)

    time_diff = int(end_time) - int(start_time)

    # print('time difference between for the sample is :' + str(time_diff) )
    # catch division by zero error

    # floor makes sure that we have sufficient data points (do not use ceil)
    ms = math.floor(float(time_diff) / float(DATA_POINTS))

    # if(ms == 0):
    #     print("ERROR : MS is ZERO")
    #     return res
    ms = str(ms) + 'ms'

    # print('Frequency is ' + str(fq))
    # print('MS time delta for interpolation is :' + ms)

    df['timeMs'] = pd.to_datetime(df['timeMs'], unit='ms')

    df_accX = df[['timeMs', 'accX']].copy()
    df_accY = df[['timeMs', 'accY']].copy()
    df_accZ = df[['timeMs', 'accZ']].copy()

    resampled_df_accX = df_accX.set_index('timeMs').resample(ms).mean().interpolate()
    resampled_df_accY = df_accY.set_index('timeMs').resample(ms).mean().interpolate()
    resampled_df_accZ = df_accZ.set_index('timeMs').resample(ms).mean().interpolate()

    # total discarded samples since ms has to be integer
    # print(resampled.size - DATA_POINTS)

    resampled_df_accX = resampled_df_accX[:DATA_POINTS]
    resampled_df_accY = resampled_df_accY[:DATA_POINTS]
    resampled_df_accZ = resampled_df_accZ[:DATA_POINTS]

    dfList_accX = list(resampled_df_accX['accX'])
    dfList_accY = list(resampled_df_accY['accY'])
    dfList_accZ = list(resampled_df_accZ['accZ'])

    # str_accX = ' '.join([str(round(s, DECIMAL_PLACES)) for s in dfList_accX])
    # str_accY = ' '.join([str(round(s, DECIMAL_PLACES)) for s in dfList_accY])
    # str_accZ = ' '.join([str(round(s, DECIMAL_PLACES)) for s in dfList_accZ])
    acc_x = [round(s, DECIMAL_PLACES) for s in dfList_accX]
    acc_y = [round(s, DECIMAL_PLACES) for s in dfList_accY]
    acc_z = [round(s, DECIMAL_PLACES) for s in dfList_accZ]

    res.append(userID)
    res.append(acc_x)
    res.append(acc_y)
    res.append(acc_z)

    # returns [userID, str(acc_x values list), str(acc_y values list), str(acc_z values list)]
    return res
