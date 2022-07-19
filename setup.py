import setuptools

setuptools.setup(
    name='wkmap2', 
    version='0.0.1',    
    package_data={'wkmap2': [
        'pipeline/testdata/*',
        'pipeline/dump_headers/*',
    ]},
    install_requires=[
        'apache-beam',
        'google-cloud-storage',
        'smart-open',
    ], 
    packages=setuptools.find_packages(),
)
