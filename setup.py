import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bill_calculator_hep",
    version="0.2.3",
    author="Maria P. Acosta F./HEPCloud project",
    author_email="macosta@fnal.gov",
    description="Billing calculations and threshold alarms for hybrid cloud setups",
    install_requires=['gcs_oauth2_boto_plugin', 'pyparsing', 'google-cloud-bigquery[pandas]'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    url="https://github.com/HEPCloud/bill-calculator-hep",
    packages=setuptools.find_packages(where='src'),
    package_dir={'': 'src', },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.4',
)
