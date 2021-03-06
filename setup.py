import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dopa",
    version="0.0.3.2",
    author="Marco Milanesio",
    author_email="milanesio.marco@gmail.com",
    description="Easily parallelize tasks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/UCA-MSI/dopa.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
