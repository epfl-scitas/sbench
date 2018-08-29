module load {{ blas }} hpl

cd {{ test_directory }}
srun xhpl
