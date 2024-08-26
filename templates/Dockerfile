# This is a Jinja2 compatible Dockerfile template
FROM scratch
LABEL maintainer="Clement Verna <cverna@fedoraproject.org>"
ENV DISTTAG={{ version }}container FGC={{ version }} FBR={{ version }}
ADD {{ tar_name }} /
CMD ["/bin/bash"]
