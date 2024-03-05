### Titanic Survival Prediction ML System


| Dynamic Data Source | Prediction Problem | UI | Monitoring
| -------- | ------- | ------- | ------- |
| Synthetic Titanic Passenger Creator | Passenger Survival | Github Pages Dashboard and Gradio Interactive UI | No monitoring


There are two ML Systems:
 
 * a batch ML system that runs daily that updates a Dashboard showing whether that day's new Titanic Passenger survived or not;
 * an interactive ML system that requires you to enter details for a hypothetical passenger and it predicts whether that passenger would survive or not.


The historical dataset used for backfilling is the titanic survival dataset.
The model used for the predictions is a binary classificator that uses XGBoost with the default hyperparameters.
