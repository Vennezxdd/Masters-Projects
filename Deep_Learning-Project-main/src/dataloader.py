import os
import numpy as np
import librosa
import torch
from torch.utils.data import Dataset
from glob import glob

def preprocess_track(track, target='vocals', chunk_duration=6.0, sr=44100,
                    n_fft=2048, hop_length=1024, save_dir='data', split='train'):
    """
    Process a single MUSDB track: mixture and vocal target.
    Save 6-second chunks as .npy (log-magnitude spectrograms).
    """
    # Load mixture and vocals (both stereo)
    mixture = track.audio.T  # shape (2, N)
    vocals = track.targets[target].audio.T

    # Calculate how many chunks there are (default 6s chunks)
    total_samples = mixture.shape[1]
    chunk_samples = int(chunk_duration * sr)
    num_chunks = total_samples // chunk_samples

    os.makedirs(f"{save_dir}/{split}/mixture/{track.name}", exist_ok=True)  # MIXTURE log-magnitude
    os.makedirs(f"{save_dir}/{split}/{target}/{track.name}", exist_ok=True) # TARGET log-magnitude
    os.makedirs(f"{save_dir}/{split}/phase/{track.name}", exist_ok=True)    # MIXTURE phase (phase-awareness)

    for i in range(num_chunks):
        start = i * chunk_samples   # 0, 264600, 529200, ...
        end = start + chunk_samples # 264600, 529200, 793800, ...

        # Extract stereo chunks
        mix_chunk = mixture[:, start:end]  # shape (2, chunk_samples)
        voc_chunk = vocals[:, start:end]

        # STFT per channel
        mix_stft = [librosa.stft(mix_chunk[ch], n_fft=n_fft, hop_length=hop_length) for ch in range(2)]
        voc_stft = [librosa.stft(voc_chunk[ch], n_fft=n_fft, hop_length=hop_length) for ch in range(2)]

        # Convert to log-magnitude
        mix_mag = np.stack([np.log1p(np.abs(stft)) for stft in mix_stft]) # shape (2, freq, time)
        voc_mag = np.stack([np.log1p(np.abs(stft)) for stft in voc_stft])

        # Phases of the mixture
        mix_phase = np.stack([np.angle(stft) for stft in mix_stft]) # shape (2, freq, time)

        # Save
        np.save(f"{save_dir}/{split}/mixture/{track.name}/{i}.npy", mix_mag)
        np.save(f"{save_dir}/{split}/{target}/{track.name}/{i}.npy", voc_mag)
        np.save(f"{save_dir}/{split}/phase/{track.name}/{i}.npy", mix_phase)

class STFTChunkDataset(Dataset):
    def __init__(self, data_dir='data', split='train', target='vocals', augment=False):
        self.mix_paths = sorted(
            glob(os.path.join(data_dir, split, 'mixture', '*', '*.npy'))
        )
        self.target_paths = [p.replace('mixture', f'{target}') for p in self.mix_paths]
        self.phase_paths = [p.replace('mixture', 'phase') for p in self.mix_paths]
        self.augment = augment

        print(f"Loaded {len(self.mix_paths)} files from:")
        print(f"- {split}/mixture")
        print(f"- {split}/{target}")
        print(f"- {split}/phase")

    def __len__(self):
        return len(self.mix_paths)

    def __getitem__(self, idx):
        mix = np.load(self.mix_paths[idx])          # shape: (2, F, T)
        target = np.load(self.target_paths[idx])    # shape: (2, F, T)
        phase = np.load(self.phase_paths[idx])      # shape: (2, F, T)

        mix = torch.tensor(mix, dtype=torch.float32)
        target = torch.tensor(target, dtype=torch.float32)
        phase = torch.tensor(phase, dtype=torch.float32)

        if self.augment:
            mix = self.apply_spec_augment(mix)

        return mix, target, phase
    
    def apply_spec_augment(self, spec, time_mask_param=15, freq_mask_param=10):
        """"
        Apply SpecAugment to the spectrogram. Set randomly the time and frequency masks to zero
        to increase the robustness of the model against noise and occlusions.
        This is done for both channels of the stereo signal.
        Args:
            spec (torch.Tensor): Spectrogram of shape (2, F, T).
            time_mask_param (int): Maximum length of time mask.
            freq_mask_param (int): Maximum length of frequency mask.
        Returns:
            torch.Tensor: Augmented spectrogram.
        """
        # Apply the same augmentations to both stereo channels
        for ch in range(2):
            # Time masking
            t = spec.shape[-1]
            t0 = torch.randint(0, t - time_mask_param, (1,)).item()
            w = torch.randint(1, time_mask_param, (1,)).item()
            spec[ch, :, t0:t0 + w] = 0

            # Frequency masking
            f = spec.shape[1]
            f0 = torch.randint(0, f - freq_mask_param, (1,)).item()
            v = torch.randint(1, freq_mask_param, (1,)).item()
            spec[ch, f0:f0 + v, :] = 0

        return spec
