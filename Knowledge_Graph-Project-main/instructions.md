# Knowledge_Graph-Project

### Locally

Start by installing miniconda on your machine
https://repo.anaconda.com/miniconda/ \
You can follow these steps if you need help
https://www.anaconda.com/docs/getting-started/miniconda/install#windows-installation

Then, you will be able to use **Anaconda Terminal Prompt** which we will use to to several things:

0. Go to your project's root directory

1. Create new conda environment
```sh
conda create --prefix .\cenv37 python=3.7
```

2. Activate venv kernel (<...> change with actual PATH)
```sh
conda activate "<...>\Knowledge_Graph-Project\cenv37"
```

3. Install all dependencies
```sh
conda install ipykernel --update-deps --force-reinstall

python -m pip install rdflib pandas pyrdf2vec gensim aiohttp requests scikit-learn torch_geometric

conda install -c conda-forge ipywidgets
```

Now you will be able to run Jupyter Notebook!

At the end, to exit from the virtual environment, just type
```sh
conda deactivate
```
or exit the terminal

### Important
Do not forget to unzip the dataset folder **before running Jupyter Notebook**!

You should install Miniconda or you will have more trouble preparing this project.

We need to run this following command (Windows 11) to be able to install "C++ build tools". This library is required to install "pyrdf2vec".
```sh
winget install Microsoft.VisualStudio.2022.BuildTools --force --override "--wait --passive --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 --add Microsoft.VisualStudio.Component.Windows11SDK.22621"
```
https://stackoverflow.com/questions/40504552/how-to-install-visual-c-build-tools
