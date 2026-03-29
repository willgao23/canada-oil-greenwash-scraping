# canada-oil-greenwash-scraping

### Requirements

Run the following commands to install the necessary code requirements:

```
pip install -r requirements.txt
```

```
python -m spacy download en_core_web_lg
```

To install tesserocr and related dependencies, follow the instructions specific to your OS on the documentation page:
https://pypi.org/project/tesserocr/

### Pipeline

Overview: scraping -> labelling -> training -> analysis
Global variables are defined in `src/config.py`

```
src/scraping
```

This directory contains the scripts use to retrieve and scrape the plain text contents of Canadian oil and gas company press releases both before and after the enactment of Bill C-59. The full scraping pipeline can be run with `python ./src/scraping/main.py`, or specific parts of the pipeline can be run by commenting out the other function calls in `main.py`.

```
src/labelling
```

This directory contains two different options for labelling the press release sentences found in the prior step: `label_cli.py` which is a basic command line interface for directly labelling a random subset of the dataset, and `rare_class_labelling.ipynb` which adapts a portion of the methodology proposed by [Mullapudi et al.](https://openaccess.thecvf.com/content/ICCV2021/papers/Mullapudi_Learning_Rare_Category_Classifiers_on_a_Tight_Labeling_Budget_ICCV_2021_paper.pdf) and uses a logistic regression model to present the user with sentences that are most likely to belong to the rare positive class for labelling. `label_cli.py` must be run first in order to get initial examples of the rare class which `rare_class_labelling.ipynb` is dependent on.

```
src/training
```

This directory contains the notebook used for fine-tuning RoBERTa on the subset of the data labelled in the previous step.

```
src/analysis
```

This directory contains the notebooks used for model inference and basic descriptive statistical analysis (`inference.ipynb`), latent dirichlet allocation for statistical topic modelling by time period and organization (`lda.ipynb`), and a script which runs each of the sentences labelled as green claims by the model through the [open web demo of VAGO](https://research.mondeca.com/demo/vago/) to analyze the extent to which they are linguistically vague.

### Sources and Generative AI Use Disclosure

Any sources used in the creation of a specific script / notebook are credited at the top of the respective file.

Generative AI was used to create the regular expressions used in the clean_sentences function of `./src/scraping/processor.py`.
