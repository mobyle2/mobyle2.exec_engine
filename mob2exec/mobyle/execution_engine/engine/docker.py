import os
from mobyle.common.config import Config

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
    :param sudo: Use sudo
    :type sudo: bool
    '''
    self.env_vars = env_vars
    self.volumes = volumes
    cfg = Config.config()
    if cfg.get("app:main", "use_sudo") and cfg.get("app:main", "use_sudo") == 'true':
      self.sudo = "sudo "
    else:
      self.sudo = ""


  def build_pull_command(self, job):
    '''
    Builds the pull command

    :param job: Current job
    :type job: Job
    '''
    container = job.service['containers'][0]
    cmd = self.sudo + 'docker pull '+container['url']+' > .'+container['type']+'.stdout'
    return cmd



  def build_run_command(self, job):
    '''
    Builds the execution command

    :param job: Current job
    :type job: Job
    '''
    cmd = ''
    # Get user and group
    cmd += 'uid=`id -u`'+"\n"
    cmd += 'gid=`id -g`'+"\n"
    container = job.service['containers'][0]
    cmd += self.sudo + 'docker run '
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
    cmd += ' bash -c "'
    # Create user and group
    cmd += 'groupadd --gid $gid mobyle && useradd --uid $uid --gid $gid mobyle;'
    # Execute command in container
    cmd += '/bin/sh '+'.'+job.service['containers'][0]['type']+'_job_script;'
    cmd += 'chown -R $uid:$gid '+job['_dir']+'"' # Reset file access rights
    return cmd
