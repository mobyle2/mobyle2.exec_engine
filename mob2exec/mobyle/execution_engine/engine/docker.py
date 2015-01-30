import os
import pwd
import grp

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
        '''
        self.env_vars = env_vars
        self.volumes = volumes
        cfg = Config.config()
        if cfg.get("app:main", "use_sudo") and cfg.get("app:main", "use_sudo") == 'true':
            self.sudo = "sudo "
        else:
            self.sudo = ""
            
        self.group_mobyle = pwd.getpwuid(os.geteuid())
        self.user_mobyle = grp.getgrgid(os.getegid())


    def build_pull_command(self, job):
        '''
        Builds the pull command

        :param job: Current job
        :type job: Job
        '''
        container = job.service['containers'][0]
        cmd = "{sudo} docker pull {container_pull} > .{container_type}.stdout".format(sudo = self.sudo,
                                                                                      container_pull = container['url'],
                                                                                      container_type = container['type'])
        return cmd



    def build_run_command(self, job):
        '''
        Builds the execution command

        :param job: Current job
        :type job: Job
        '''
        
        container = job.service['containers'][0]
        
        volumes = ''
        if self.volumes:
            for vol in self.volumes:
                volumes += ' -v {0}:{0}'.format(vol)
        
        env = ''
        if self.env_vars:
            for key, value in self.env_vars.iteritems():
                env = ' -e "{0}={1}"'.format(key, value)
                
        cmd = '''uid={uid}
gid={gid}
{sudo} docker run --rm -w {job_dir} -v {job_dir}:{job_dir} {volumes}{env} {container_id} \ 
bash -c "groupadd --gid {gid} {group_mobyle} && useradd --uid {uid} --gid {gid} {user_mobyle}; \
/bin/sh .{container_type}_job_script; \
chown -R {uid}:{gid} {job_dir}"'''.format(uid = os.getuid(),
                                          gid = os.getgid(),
                                          job_dir = job.dir,
                                          volumes = volumes,
                                          env = env,
                                          container_id = container['id'],
                                          group_mobyle = self.group_mobyle,
                                          user_mobyle = self.user_mobyle,
                                          container_type = container['type'],
                                          )
        return cmd
