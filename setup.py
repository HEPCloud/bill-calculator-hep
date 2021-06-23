import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bill-calculator-hep", # This needs to change soon
    version="0.0.10",
    author="Maria P. Acosta F./HEPCloud project",
    author_email="macosta@fnal.gov",
    description="Billing calculations and threshold alarms for hybrid cloud setups",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    url="https://github.com/HEPCloud/billing-calculator",
    packages=setuptools.find_packages(),
    package_dir={'lib': './lib',},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.4',
)
