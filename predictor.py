import pandas as pd
import matplotlib.pyplot as plt

tweets = pd.read_csv('tweets.csv')

# Plot the number of tweets by sentiment
tweets.groupby('sentiment').count()['tweet'].plot.bar()
plt.show()

