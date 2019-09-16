# Raw Data

A sample of a [record linkage study](https://archive.ics.uci.edu/ml/datasets/record+linkage+comparison+patterns) that was performed at a German hospital
in 2010.

## Notes

To retrieve the data, please run the following commands from the Terminal:

```bash
# download data
wget http://bit.ly/1Aoywaq -O donations.zip

unzip donations.zip

unzip 'block_*.zip'

# stack all of the block_*.csv files into one newly created linkage.csv file
csvstack --filenames block_1.csv block_2.csv block_3.csv block_4.csv block_5.csv block_6.csv block_7.csv block_8.csv block_9.csv block_10.csv > linkage.csv
```

The final `linkage.csv` will contain the following 13 column headers:

```
group,id_1,id_2,cmp_fname_c1,cmp_fname_c2,cmp_lname_c1,cmp_lname_c2,cmp_sex,cmp_bd,cmp_bm,cmp_by,cmp_plz,is_match
```
