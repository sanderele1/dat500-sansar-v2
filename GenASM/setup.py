from distutils.core import setup, Extension

module = Extension("gasm", sources=["genasm_aligner.c"])

setup(
	name="gasm",
	version="1.1.1.111",
	description="Gasm but in Python but still in C!",
	ext_modules = [module]
)
