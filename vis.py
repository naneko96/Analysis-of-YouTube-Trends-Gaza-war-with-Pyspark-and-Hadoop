import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

fig, axes = plt.subplots(1, 2, figsize=(15, 6))


keywords = pd.read_csv("top_keywords.csv")
sns.barplot(ax=axes[0], x='count', y='word', data=keywords, palette='viridis')
axes[0].set_title("Top Theme Keywords")


sentiments = pd.read_csv("sentiment_results.csv")
axes[1].pie(sentiments['count'], labels=sentiments['sentiment'], autopct='%1.1f%%', colors=['#ff9999','#66b3ff','#99ff99'])
axes[1].set_title("Dominant Sentiment in Comments")

plt.tight_layout()
plt.savefig("gaza_final_dashboard.png")
print(" DASHBOARD CREATED: gaza_final_dashboard.png")