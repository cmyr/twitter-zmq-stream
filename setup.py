from setuptools import setup

setup(name='zmqstream',
      version='0.1',
      description='stream iterators over zmq',
      author='Colin Rofls',
      author_email='colin@cmyr.net',
      url='https://github.com/cmyr/twitter-zmq-stream',
      install_requires=['pyzmq', 'requests-oauthlib'],
      scripts=['bin/samplepublisher.py'],
      packages=['zmqstream']
      )
