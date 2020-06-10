import setuptools

setuptools.setup(
    name="doralite",
    version="1.0.0",
    author="John Krasting",
    author_email="John.Krasting@noaa.gov",
    description="A lightweight package for interacting with Dora",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    url="https://github.com/jkrasting/doralite",
    scripts=["scripts/dora"],
    packages=setuptools.find_packages(),
)
