import os

class DockerContainer(object):
  '''
  Manage Docker container commands
  '''

  def __init__(self, env_vars=None, volumes=None):
    '''
    Define a container

    :param env_vars: List of env variables to share in container
    :type env: dict
    :param volumes: list of volumes to mount in addition to job directory
    :type volumes: list
    '''
    self.env_vars = env_vars
    self.volumes = volumes


  def build_pull_command(self, job):
    '''
    Builds the pull command

    :param job: Current job
    :type job: Job
    '''
    container = job.service['containers'][0]
    cmd = 'sudo docker pull '+container['url']+' > .'+container['type']+'.stdout'
    return cmd



  def build_run_command(self, job):
    '''
    Builds the execution command

    :param job: Current job
    :type job: Job
    '''
    cmd = ''
    # Create user and group
    cmd += 'uid=`id -u`'+"\n"
    cmd += 'gid=`id -g`'+"\n"
    container = job.service['containers'][0]
    cmd += 'sudo docker run '
    cmd += ' --rm'  #For deletion after execution for cleanup
    cmd += ' -w '+job['_dir'] # Workdir
    cmd += ' -v '+job['_dir']+':'+job['_dir'] #Mount job directory as a shared directory
    if self.volumes:
      for vol in self.volumes:
        cmd += ' -v '+vol+':'+vol
    if self.env_vars:
      for key, value in self.env_vars.iteritems():
        cmd += ' -e "'+key+'='+value+'"'
    cmd += ' '+container['id'] # Image Id
    cmd += 'groupadd --gid $gid mobyle && useradd --uid $uid --gid $gid mobyle;'
    cmd += '/bin/sh '+'.'+job.service['containers'][0]['type']+'_job_script' # Cmd
    return cmd
