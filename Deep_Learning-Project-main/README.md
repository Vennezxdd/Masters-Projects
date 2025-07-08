# Deep_Learning-Project

### Locally (Powershell)

Start by installing miniconda on your machine
https://repo.anaconda.com/miniconda/ \
You can follow these steps if you need help
https://www.anaconda.com/docs/getting-started/miniconda/install#windows-installation

Then, you will be able to use **Anaconda Terminal Prompt** which we will use to to several things:

0. Go to your project's root directory

1. Create new conda environment (in your project root directory)
```ps
conda create --prefix .\cenv
```

2. Activate cenv kernel (write absolute *PATH*)
```ps
conda activate "C:\Users\<...>\Deep_Learning-Project\cenv"
```

3. Install all dependencies
```ps
conda install -p 'C:\Users\<...>\Deep_Learning-Project\cenv' ipykernel --update-deps --force-reinstall

conda install pytorch torchvision torchaudio pytorch-cuda -c pytorch -c nvidia

conda install -c conda-forge ffmpeg

conda install -c conda-forge stempeg

python -m pip install musdb museval numpy==1.26.4 librosa matplotlib openunmix tensorboard

python -m pip install qrcode[pil]

python -m pip install torchmetrics
```

Now you will be able to run Jupyter Notebook!

At the end, to exit from the conda environment, just type
```ps
conda deactivate
```

### Important
Each person should be responsible for downloading the full dataset ***musdb18hq*** locally. You can download it here \
        https://zenodo.org/records/3338373 \
With a good wifi speed it will take around 20 minutes (or a bit more). Then, you will need to uncompress it and add it to the project's root directory.

When you run the Jupyter notebook, new files will appear, such as:
- **logs** (directory with previous trained models, used by TensorBoard) \
        - If you want to remove old files, you first need to close any executing TensorBoard instances!
- **\_\_pycache\_\_** (inside **src** folder, used to improve performance when loading the Dataset)
