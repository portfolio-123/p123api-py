import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="p123api",
    version="1.4.0",
    author="Portfolio123",
    author_email="info@portfolio123.com",
    description="Portfolio123 API wrapper",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/portfolio-123/p123api-py",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
