# coding=utf-8

# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

# this code assumes you are working from an interactive Thunder (PySpark) shell

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
plt.ion()

##################
# data exploration
##################

# load some zebrafish brain data
path_to_images = ('path/to/thunder/python/thunder/utils/data/fish/tif-stack')
imagesRDD = tsc.loadImages(path_to_images, inputformat='tif-stack')

# explore the resulting object
print imagesRDD
print imagesRDD.rdd
print imagesRDD.first()
print imagesRDD.first()[1].shape
print imagesRDD.dims
print imagesRDD.nimages

# plot the raw data
img = imagesRDD.values().first()
plt.imshow(img[:, : ,0], interpolation='nearest', aspect='equal', cmap='gray')

# plot subsampled data
subsampled = imagesRDD.subsample((5, 5, 1))
plt.imshow(subsampled.first()[1][:, : ,0], interpolation='nearest', aspect='equal', cmap='gray')
print subsampled.dims

# reshuffle data to series representation
seriesRDD = imagesRDD.toSeries()
print seriesRDD.dims
print seriesRDD.index
print seriesRDD.count()
print seriesRDD.rdd.takeSample(False, 1, 0)[0]
print seriesRDD.max()

# distributed computation of stats
stddevRDD = seriesRDD.seriesStdev()
print stddevRDD.take(3)
print stddevRDD.dims

# collecting data locally and repacking it
repacked = stddevRDD.pack()
plt.imshow(repacked[:,:,0], interpolation='nearest', cmap='gray', aspect='equal')
print type(repacked)
print repacked.shape

# plot some of the time series themselves
plt.plot(seriesRDD.center().subset(50).T)

# distributed computatino of custom statistics
seriesRDD.apply(lambda x: x.argmin())


###############################
# Clustering fish brain regions
###############################

import numpy as np
from thunder import KMeans

seriesRDD = tsc.loadSeries('path/to/thunder/python/thunder/utils/data/fish/bin')
print seriesRDD.dims
print seriesRDD.index

normalizedRDD = seriesRDD.normalize(baseline='mean')
stddevs = (normalizedRDD
    .seriesStdev()
    .values()
    .sample(False, 0.1, 0)
    .collect())
plt.hist(stddevs, bins=20)
plt.plot(normalizedRDD.subset(50, thresh=0.1, stat='std').T)

# perform k-means on the normalized series
ks = [5, 10, 15, 20, 30, 50, 100, 200]
models = []
for k in ks:
    models.append(KMeans(k=k).fit(normalizedRDD))

# define a couple functions to score the clustering quality
def model_error_1(model):
    def series_error(series):
        cluster_id = model.predict(series)
        center = model.centers[cluster_id]
        diff = center - series
        return diff.dot(diff) ** 0.5
    
    return normalizedRDD.apply(series_error).sum()

def model_error_2(model):
    return 1. / model.similarity(normalizedRDD).sum()

# compute the error metrics for the different resulting clusterings
errors_1 = np.asarray(map(model_error_1, models))
errors_2 = np.asarray(map(model_error_2, models))
plt.plot(
    ks, errors_1 / errors_1.sum(), 'k-o',
    ks, errors_2 / errors_2.sum(), 'b:v')

# plot the best performing model
model20 = models[3]
plt.plot(model20.centers.T)

# finally, plot each brain region according to its characteristic behavior
by_cluster = model20.predict(normalizedRDD).pack()
cmap_cat = ListedColormap(sns.color_palette("hls", 10), name='from_list')
plt.imshow(by_cluster[:, :, 0], interpolation='nearest', aspect='equal', cmap='gray')
