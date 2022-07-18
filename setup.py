import setuptools

setuptools.setup(
    name='wkmap2', 
    version='0.0.1', 
    install_requires=[
        'apache-beam',
        'smart-open',
        'google-cloud-storage',
    ], 
    packages=setuptools.find_packages(),
)
