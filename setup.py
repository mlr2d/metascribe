from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name='metascribe',
    version="1.0.0",
    description="Utility to match arbirrary files against Jinja2 templates and extract variable values.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='',
    author='Aravind Sankaran',
    author_email='aravindsankaran22@gmail.com',
    packages= find_packages(), # finds packages inside current directory
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">3",
    install_requires=open("requirements.txt").read().splitlines(),
    entry_points={
        'console_scripts': [
            'md-parser=metascribe.cli:md_parser',
            'md-store=metascribe.cli:md_store',
        ],
    },
)
