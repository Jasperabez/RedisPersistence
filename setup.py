import setuptools

with open("README.md", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name="redis-persistence",
	version="0.0.4",
	author="Moris Doratiotto",
	author_email="moris.doratiotto@gmail.com",
	description="A python module to make your Telegram bot persistent using Redis",
	long_description=long_description,
	long_description_content_type="text/markdown",
	url="https://github.com/Mortafix/RedisPersistence",
	packages=setuptools.find_packages(),
	install_requires=["redis"],
	classifiers=[
		"Programming Language :: Python :: 3.8",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
		"Development Status :: 3 - Alpha",
		"Intended Audience :: Developers",
	],
	python_requires='>=3.8',
	keywords=['persistence', 'persistent', 'redis', 'telegram', 'bot'],
)