FROM codercom/code-server:latest

# Switch to root to install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y build-essential curl gdb lldb && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the default coder user
USER coder

# Install Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
ENV PATH="/home/coder/.cargo/bin:${PATH}"
RUN echo 'source $HOME/.cargo/env' >> /home/coder/.bashrc

# Pre-install ONLY the essential VS Code extensions for the exam
# 1. rust-analyzer (Provides method lists, syntax highlighting, basic IntelliSense)
# 2. CodeLLDB (Provides native debugging capabilities)
# 3. Code Runner (Allows quick running with a play button)
# 4. vscode-pdf (Allows you to read PDF tracks directly inside the code editor)
RUN code-server --install-extension rust-lang.rust-analyzer && \
    code-server --install-extension vadimcn.vscode-lldb && \
    code-server --install-extension formulahendry.code-runner && \
    code-server --install-extension tomoki1207.pdf

# Create the workspace directory
RUN mkdir -p /home/coder/project

# Set default working directory
WORKDIR /home/coder/project
