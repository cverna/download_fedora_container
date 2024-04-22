# This is a Jinja2 compatible Dockerfile template
FROM scratch
LABEL maintainer="Clement Verna <cverna@fedoraproject.org>"
ENV DISTTAG={{ version }}container FGC={{ version }} FBR={{ version }}
ADD layer.tar /
CMD ["/bin/bash"]
